/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package remote

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	jsoniter "github.com/json-iterator/go"
)

var opaque int32

const (
	// 0, REQUEST_COMMAND
	RPCType = 0
	// 1, RPC
	RPCOneWay = 1
	//ResponseType for response
	ResponseType = 1
	_Flag        = 0
	_Version     = 317
)

type LanguageCode byte

const (
	_Java    = LanguageCode(0)
	_Go      = LanguageCode(9)
	_Unknown = LanguageCode(127)
)

func (lc LanguageCode) MarshalJSON() ([]byte, error) {
	return []byte(`"GO"`), nil
}

func (lc *LanguageCode) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case "JAVA":
		*lc = _Java
	case "GO", `"GO"`:
		*lc = _Go
	default:
		*lc = _Unknown
	}
	return nil
}

func (lc LanguageCode) String() string {
	switch lc {
	case _Java:
		return "JAVA"
	case _Go:
		return "GO"
	default:
		return "unknown"
	}
}

// RemotingCommand 用于通信协议的数据结构，主要用于客户端和服务端之间的消息传递和指令交互
type RemotingCommand struct {
	Code      int16             `json:"code"`      // 是一个整型数值，表示远程命令的类型或请求的操作码。不同的 code 代表不同的操作，比如消息发送、消息拉取、心跳等。它用于区分不同类型的命令。
	Language  LanguageCode      `json:"language"`  // 是一个枚举类型，表示请求的客户端语言。RocketMQ 支持多种客户端语言（如 Java、Go、等），这个字段帮助服务端识别客户端的语言类型，以确保通信的兼容性
	Version   int16             `json:"version"`   // 表示协议的版本号。这用于保证客户端和服务端之间的协议兼容性，不同版本的协议可能有不同的字段或处理逻辑
	Opaque    int32             `json:"opaque"`    // 是一个唯一标记符，用于标识一条请求消息。客户端发送请求时会生成一个opaque值，服务端在响应时会带上相同的值，以便客户端能够将响应与请求一一对应。这相当于一个请求的标识 ID。
	Flag      int32             `json:"flag"`      // 是一个标志位，表示请求的额外状态或控制信息。它通常用作一些特殊功能的开关或标志，比如是否需要压缩、加密等
	Remark    string            `json:"remark"`    // 是一个备注字段，用于存储附加的描述信息，通常在出错或特殊情况下，返回额外的提示或错误信息。
	ExtFields map[string]string `json:"extFields"` // 是一个扩展字段的键值对映射。这个字段用于传递额外的业务或控制信息，键是字符串，值也是字符串。这部分信息是可扩展的，具体内容根据不同的业务需求而定
	Body      []byte            `json:"-"`         // 消息的实际数据，通常是二进制形式的负载。这个字段可能会包含需要传输的核心业务数据，比如消息内容或其他序列化的数据
}

type CustomHeader interface {
	Encode() map[string]string
}

func NewRemotingCommand(code int16, header CustomHeader, body []byte) *RemotingCommand {
	cmd := &RemotingCommand{
		Code:      code,
		Version:   _Version,
		Opaque:    atomic.AddInt32(&opaque, 1),
		Body:      body,
		Language:  _Go,
		ExtFields: make(map[string]string),
	}

	if header != nil {
		cmd.ExtFields = header.Encode()
	}

	return cmd
}

func (command *RemotingCommand) String() string {
	return fmt.Sprintf("Code: %d, opaque: %d, Remark: %s, ExtFields: %v",
		command.Code, command.Opaque, command.Remark, command.ExtFields)
}

func (command *RemotingCommand) isResponseType() bool {
	return command.Flag&(ResponseType) == ResponseType
}

func (command *RemotingCommand) markResponseType() {
	command.Flag = command.Flag | ResponseType
}

var (
	jsonSerializer     = &jsonCodec{}
	rocketMqSerializer = &rmqCodec{}
	codecType          byte
)

// encode RemotingCommand
//
// Frame format:
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// + item | frame_size | header_length |         header_body        |     body     +
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// + len  |   4bytes   |     4bytes    | (21 + r_len + e_len) bytes | remain bytes +
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
func (command *RemotingCommand) WriteTo(w io.Writer) error {
	var (
		header []byte
		err    error
	)

	switch codecType {
	case JsonCodecs:
		header, err = jsonSerializer.encodeHeader(command)
	case RocketMQCodecs:
		header, err = rocketMqSerializer.encodeHeader(command)
	}

	if err != nil {
		return err
	}

	frameSize := 4 + len(header) + len(command.Body)
	err = binary.Write(w, binary.BigEndian, int32(frameSize))
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, markProtocolType(int32(len(header))))
	if err != nil {
		return err
	}

	_, err = w.Write(header)
	if err != nil {
		return err
	}

	_, err = w.Write(command.Body)
	return err
}

func encode(command *RemotingCommand) ([]byte, error) {
	var (
		header []byte
		err    error
	)

	switch codecType {
	case JsonCodecs:
		header, err = jsonSerializer.encodeHeader(command)
	case RocketMQCodecs:
		header, err = rocketMqSerializer.encodeHeader(command)
	}

	if err != nil {
		return nil, err
	}

	frameSize := 4 + len(header) + len(command.Body)
	buf := bytes.NewBuffer(make([]byte, frameSize))
	buf.Reset()

	err = binary.Write(buf, binary.BigEndian, int32(frameSize))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, markProtocolType(int32(len(header))))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, header)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, command.Body)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decode(data []byte) (*RemotingCommand, error) {
	buf := bytes.NewReader(data)
	length := int32(len(data))
	var oriHeaderLen int32
	err := binary.Read(buf, binary.BigEndian, &oriHeaderLen)
	if err != nil {
		return nil, err
	}

	headerLength := oriHeaderLen & 0xFFFFFF
	headerData := make([]byte, headerLength)
	if _, err = io.ReadFull(buf, headerData); err != nil {
		return nil, err
	}

	var command *RemotingCommand
	switch codeType := byte((oriHeaderLen >> 24) & 0xFF); codeType {
	case JsonCodecs:
		command, err = jsonSerializer.decodeHeader(headerData)
	case RocketMQCodecs:
		command, err = rocketMqSerializer.decodeHeader(headerData)
	default:
		err = fmt.Errorf("unknown codec type: %d", codeType)
	}
	if err != nil {
		return nil, err
	}

	bodyLength := length - 4 - headerLength
	if bodyLength > 0 {
		bodyData := make([]byte, bodyLength)
		if _, err = io.ReadFull(buf, bodyData); err != nil {
			return nil, err
		}
		command.Body = bodyData
	}
	return command, nil
}

func markProtocolType(source int32) []byte {
	result := make([]byte, 4)
	result[0] = codecType
	result[1] = byte((source >> 16) & 0xFF)
	result[2] = byte((source >> 8) & 0xFF)
	result[3] = byte(source & 0xFF)
	return result
}

const (
	JsonCodecs     = byte(0)
	RocketMQCodecs = byte(1)
)

type serializer interface {
	encodeHeader(command *RemotingCommand) ([]byte, error)
	decodeHeader(data []byte) (*RemotingCommand, error)
}

// jsonCodec please refer to remoting/protocol/RemotingSerializable
type jsonCodec struct{}

func (c *jsonCodec) encodeHeader(command *RemotingCommand) ([]byte, error) {
	buf, err := jsoniter.Marshal(command)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *jsonCodec) decodeHeader(header []byte) (*RemotingCommand, error) {
	command := &RemotingCommand{}
	command.ExtFields = make(map[string]string)
	command.Body = make([]byte, 0)
	err := jsoniter.Unmarshal(header, command)
	if err != nil {
		return nil, err
	}
	return command, nil
}

// rmqCodec implementation of RocketMQCodecs private protocol, please refer to remoting/protocol/RocketMQSerializable
// RocketMQCodecs Private Protocol Header format:
//
// v_flag: version flag
// r_len: length of remark body
// r_body: data of remark body
// e_len: length of extends fields body
// e_body: data of extends fields
//
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// + item | request_code | l_flag | v_flag | opaque | request_flag |  r_len  |   r_body    |  e_len  |    e_body   +
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// + len  |    2bytes    |  1byte | 2bytes | 4bytes |    4 bytes   | 4 bytes | r_len bytes | 4 bytes | e_len bytes +
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
const (
	// header + body length
	headerFixedLength = 21
)

type rmqCodec struct{}

// encodeHeader
func (c *rmqCodec) encodeHeader(command *RemotingCommand) ([]byte, error) {
	extBytes, err := c.encodeMaps(command.ExtFields)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(make([]byte, headerFixedLength+len(command.Remark)+len(extBytes)))
	buf.Reset()

	// request code, length is 2 bytes
	err = binary.Write(buf, binary.BigEndian, int16(command.Code))
	if err != nil {
		return nil, err
	}

	// language flag, length is 1 byte
	err = binary.Write(buf, binary.BigEndian, _Go)
	if err != nil {
		return nil, err
	}

	// version flag, length is 2 bytes
	err = binary.Write(buf, binary.BigEndian, int16(command.Version))
	if err != nil {
		return nil, err
	}

	// opaque flag, opaque is request identifier, length is 4 bytes
	err = binary.Write(buf, binary.BigEndian, command.Opaque)
	if err != nil {
		return nil, err
	}

	// request flag, length is 4 bytes
	err = binary.Write(buf, binary.BigEndian, command.Flag)
	if err != nil {
		return nil, err
	}

	// remark length flag, length is 4 bytes
	err = binary.Write(buf, binary.BigEndian, int32(len(command.Remark)))
	if err != nil {
		return nil, err
	}

	// write remark, len(command.Remark) bytes
	if len(command.Remark) > 0 {
		err = binary.Write(buf, binary.BigEndian, []byte(command.Remark))
		if err != nil {
			return nil, err
		}
	}

	err = binary.Write(buf, binary.BigEndian, int32(len(extBytes)))
	if err != nil {
		return nil, err
	}

	if len(extBytes) > 0 {
		err = binary.Write(buf, binary.BigEndian, extBytes)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (c *rmqCodec) encodeMaps(maps map[string]string) ([]byte, error) {
	if maps == nil || len(maps) == 0 {
		return []byte{}, nil
	}
	extFieldsBuf := bytes.NewBuffer([]byte{})
	var err error
	for key, value := range maps {
		err = binary.Write(extFieldsBuf, binary.BigEndian, int16(len(key)))
		if err != nil {
			return nil, err
		}
		err = binary.Write(extFieldsBuf, binary.BigEndian, []byte(key))
		if err != nil {
			return nil, err
		}

		err = binary.Write(extFieldsBuf, binary.BigEndian, int32(len(value)))
		if err != nil {
			return nil, err
		}
		err = binary.Write(extFieldsBuf, binary.BigEndian, []byte(value))
		if err != nil {
			return nil, err
		}
	}
	return extFieldsBuf.Bytes(), nil
}

func (c *rmqCodec) decodeHeader(data []byte) (*RemotingCommand, error) {
	var err error
	command := &RemotingCommand{}
	buf := bytes.NewBuffer(data)
	var code int16
	err = binary.Read(buf, binary.BigEndian, &code)
	if err != nil {
		return nil, err
	}
	command.Code = code

	var (
		languageCode byte
		remarkLen    int32
		extFieldsLen int32
	)
	err = binary.Read(buf, binary.BigEndian, &languageCode)
	if err != nil {
		return nil, err
	}
	command.Language = LanguageCode(languageCode)

	var version int16
	err = binary.Read(buf, binary.BigEndian, &version)
	if err != nil {
		return nil, err
	}
	command.Version = version

	// int opaque
	err = binary.Read(buf, binary.BigEndian, &command.Opaque)
	if err != nil {
		return nil, err
	}

	// int flag
	err = binary.Read(buf, binary.BigEndian, &command.Flag)
	if err != nil {
		return nil, err
	}

	// String remark
	err = binary.Read(buf, binary.BigEndian, &remarkLen)
	if err != nil {
		return nil, err
	}

	if remarkLen > 0 {
		var remarkData = make([]byte, remarkLen)
		if _, err = io.ReadFull(buf, remarkData); err != nil {
			return nil, err
		}
		command.Remark = string(remarkData)
	}

	err = binary.Read(buf, binary.BigEndian, &extFieldsLen)
	if err != nil {
		return nil, err
	}

	if extFieldsLen > 0 {
		extFieldsData := make([]byte, extFieldsLen)
		if _, err := io.ReadFull(buf, extFieldsData); err != nil {
			return nil, err
		}

		command.ExtFields = make(map[string]string)
		buf := bytes.NewBuffer(extFieldsData)
		var (
			kLen int16
			vLen int32
		)
		for buf.Len() > 0 {
			err = binary.Read(buf, binary.BigEndian, &kLen)
			if err != nil {
				return nil, err
			}

			key, err := getExtFieldsData(buf, int32(kLen))
			if err != nil {
				return nil, err
			}

			err = binary.Read(buf, binary.BigEndian, &vLen)
			if err != nil {
				return nil, err
			}

			value, err := getExtFieldsData(buf, vLen)
			if err != nil {
				return nil, err
			}
			command.ExtFields[key] = value
		}
	}

	return command, nil
}

func getExtFieldsData(buff io.Reader, length int32) (string, error) {
	var data = make([]byte, length)
	if _, err := io.ReadFull(buff, data); err != nil {
		return "", err
	}

	return string(data), nil
}
