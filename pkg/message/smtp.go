package message

import (
	"net/smtp"
	"net/textproto"

	"github.com/jordan-wright/email"
)

func EmailPush(addrs []string, summary, content string) error {
	e := &email.Email{
		From:    "nimitz <liujunlin@111.com>",
		To:      addrs,
		Subject: summary,
		Text:    []byte(content),
		Headers: textproto.MIMEHeader{},
	}

	return e.Send("smtp.111.com:25", smtp.PlainAuth("", "liujunlin@111.com", "JZKGhcNZbUwfbmZm", "smtp.111.com"))
}
