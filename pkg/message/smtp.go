package message

import (
	"fmt"
	"net/smtp"
	"net/textproto"

	"github.com/jordan-wright/email"
)

func Send(summary, content string) error {
	e := &email.Email{
		From:    "nimitz <liujunlin@111.com>",
		To:      []string{"liu1771451233@gmail.com"},
		Subject: summary,
		Text:    []byte(content),
		Headers: textproto.MIMEHeader{},
	}
	fmt.Println("send")

	return e.Send("smtp.111.com:25", smtp.PlainAuth("", "liujunlin@111.com", "JZKGhcNZbUwfbmZm", "smtp.111.com"))
}
