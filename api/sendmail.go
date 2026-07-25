package handler

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/smtp"
	"sync"
	"time"
)

const timeout = 10 * time.Second

// 请求结构
type SendRequest struct {
	SmtpHost string `json:"smtpHost"`
	Port     string `json:"port"`
	From     string `json:"from"`
	Password string `json:"pwd"`
	To       string `json:"to"`
	Subject  string `json:"subject"`
	Body     string `json:"body"`
}

// 响应结构
type SendResponse struct {
	Code    int    `json:"code"` // 0 成功，1 失败
	Message string `json:"message"`
}

// ---------- SMTP 连接池 ----------
type smtpConnPool struct {
	mutex     sync.Mutex
	idleConns []*smtp.Client
	addr      string
	host      string
	auth      smtp.Auth
	maxIdle   int
}

var globalPool = make(map[string]*smtpConnPool)
var poolLock sync.RWMutex

func getPoolKey(host, port, from string) string {
	return fmt.Sprintf("%s:%s|%s", host, port, from)
}

func getOrCreatePool(host, port, from, pwd string) *smtpConnPool {
	key := getPoolKey(host, port, from)
	poolLock.RLock()
	p, ok := globalPool[key]
	poolLock.RUnlock()
	if ok {
		return p
	}

	poolLock.Lock()
	defer poolLock.Unlock()
	if p, ok = globalPool[key]; ok {
		return p
	}

	addr := fmt.Sprintf("%s:%s", host, port)
	auth := smtp.PlainAuth("", from, pwd, host)
	p = &smtpConnPool{
		addr:    addr,
		host:    host,
		auth:    auth,
		maxIdle: 2,
	}
	globalPool[key] = p
	return p
}

func (p *smtpConnPool) newClient() (*smtp.Client, error) {
	tlsConf := &tls.Config{
		ServerName: p.host,
	}
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := tls.DialWithDialer(dialer, "tcp", p.addr, tlsConf)
	if err != nil {
		return nil, err
	}
	return smtp.NewClient(conn, p.host)
}

func (p *smtpConnPool) getClient() (*smtp.Client, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for len(p.idleConns) > 0 {
		cli := p.idleConns[len(p.idleConns)-1]
		p.idleConns = p.idleConns[:len(p.idleConns)-1]
		if err := cli.Noop(); err == nil {
			return cli, nil
		}
		_ = cli.Close()
	}
	return p.newClient()
}

func (p *smtpConnPool) putClient(cli *smtp.Client, usable bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !usable {
		_ = cli.Close()
		return
	}
	if len(p.idleConns) < p.maxIdle {
		p.idleConns = append(p.idleConns, cli)
		return
	}
	_ = cli.Close()
}

// ---------- 邮件发送核心 ----------
func sendMail(req SendRequest) SendResponse {
	pool := getOrCreatePool(req.SmtpHost, req.Port, req.From, req.Password)
	cli, err := pool.getClient()
	if err != nil {
		return SendResponse{Code: 1, Message: fmt.Sprintf("获取连接失败: %v", err)}
	}

	usable := true
	defer pool.putClient(cli, usable)

	// 编码主题
	encSubject := fmt.Sprintf("=?UTF-8?B?%s?=", base64.StdEncoding.EncodeToString([]byte(req.Subject)))
	msg := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nContent-Type: text/plain;charset=utf-8\r\n\r\n%s",
		req.From, req.To, encSubject, req.Body,
	)

	if err = cli.Auth(pool.auth); err != nil {
		usable = false
		return SendResponse{Code: 1, Message: fmt.Sprintf("认证失败: %v", err)}
	}
	if err = cli.Mail(req.From); err != nil {
		usable = false
		return SendResponse{Code: 1, Message: fmt.Sprintf("MAIL命令失败: %v", err)}
	}
	if err = cli.Rcpt(req.To); err != nil {
		usable = false
		return SendResponse{Code: 1, Message: fmt.Sprintf("RCPT命令失败: %v", err)}
	}
	w, errData := cli.Data()
	if errData != nil {
		usable = false
		return SendResponse{Code: 1, Message: fmt.Sprintf("DATA命令失败: %v", errData)}
	}
	_, errWrite := w.Write([]byte(msg))
	_ = w.Close()
	if errWrite != nil {
		usable = false
		return SendResponse{Code: 1, Message: fmt.Sprintf("写入邮件内容失败: %v", errWrite)}
	}

	return SendResponse{Code: 0, Message: "success"}
}

// ---------- Vercel Serverless Handler ----------
func Handler(w http.ResponseWriter, r *http.Request) {
	// 只允许 POST
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SendRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// 简单校验
	if req.SmtpHost == "" || req.Port == "" || req.From == "" || req.Password == "" ||
		req.To == "" || req.Subject == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// 发送邮件
	resp := sendMail(req)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
