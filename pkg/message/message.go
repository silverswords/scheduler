package message

import (
	"fmt"

	"github.com/wxpusher/wxpusher-sdk-go"
	"github.com/wxpusher/wxpusher-sdk-go/model"
)

const (
	appToken = "AT_lXli0tFj07JDafUASdG48eZlmy2EHsbS"
	topicID  = 2943
)

func Push(summary, content string) error {
	msg := model.NewMessage(appToken).SetContent(content).SetSummary(summary).AddTopicId(topicID)
	resps, err := wxpusher.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Println(resps)

	return nil
}
