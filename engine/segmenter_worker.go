package engine

import (
	"github.com/huichen/wukong/types"
	"log"
)

type segmenterRequest struct {
	docId uint64
	hash  uint32
	data  types.DocumentIndexData
}

func (engine *Engine) segmenterWorker() {
	for {
		request := <-engine.segmenterChannel
		shard := engine.getShard(request.hash)

		tokensMap := make(map[string][]int)
		numTokens := 0
		if !engine.initOptions.NotUsingSegmenter && request.data.Content != "" {
			// 当文档正文不为空时，优先从内容分词中得到关键词
			segments := engine.segmenter.Segment([]byte(request.data.Content))
			for _, segment := range segments {
				token := segment.Token().Text()
				if !engine.stopTokens.IsStopToken(token) {
					tokensMap[token] = append(tokensMap[token], segment.Start())
				}

				//TODO 是否需要遍历 segment.Token().Segments()
				// 该分词文本可以进一步进行分词划分，比如"中华人民共和国中央人民政府"这个分词
				// 有两个子分词"中华人民共和国"和"中央人民政府"。子分词也可以进一步有子分词
				// 形成一个树结构，遍历这个树就可以得到该分词的所有细致分词划分，这主要
				// 用于搜索引擎对一段文本进行全文搜索。
			}
			numTokens = len(segments)
		} else {
			// 否则载入用户输入的关键词
			for _, t := range request.data.Tokens {
				if !engine.stopTokens.IsStopToken(t.Text) {
					tokensMap[t.Text] = t.Locations
				}
			}
			numTokens = len(request.data.Tokens)
		}

		// 加入非分词的文档标签
		for _, label := range request.data.Labels {
			if !engine.initOptions.NotUsingSegmenter {
				if !engine.stopTokens.IsStopToken(label) {
					tokensMap[label] = []int{}
				}
			} else {
				tokensMap[label] = []int{}
			}
		}

		indexerRequest := indexerAddDocumentRequest{
			document: &types.DocumentIndex{
				DocId:       request.docId,
				TokenLength: float32(numTokens),
				Keywords:    make([]types.KeywordIndex, len(tokensMap)),
			},
		}
		iTokens := 0
		tokens := ""
		for k, v := range tokensMap {
			indexerRequest.document.Keywords[iTokens] = types.KeywordIndex{
				Text: k,
				// 非分词标注的词频设置为0，不参与tf-idf计算
				Frequency: float32(len(v)),
				Starts:    v}
			iTokens++
			tokens += k
			tokens += "|"
		}
		log.Printf("tokens=[%v] %v", tokens, request.data.Content)
		engine.indexerAddDocumentChannels[shard] <- indexerRequest

		//TODO 是否应该加入 request.data.Fields != nil 的判断才做这个操作？？
		rankerRequest := rankerAddDocRequest{
			docId: request.docId, fields: request.data.Fields}
		engine.rankerAddDocChannels[shard] <- rankerRequest
	}
}
