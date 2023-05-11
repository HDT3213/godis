package database

import (
	"fmt"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/wildcard"
	"github.com/hdt3213/godis/redis/protocol"
	"reflect"
	"strconv"
	"strings"
)

func init() {

}

type configCmd struct {
	name      string
	operation string
	executor  ExecFunc
}

var configCmdTable = make(map[string]*configCmd)

func execConfigCommand(args [][]byte) redis.Reply {
	if len(args) == 0 {
		return getAllGodisCommandReply()
	}
	subCommand := strings.ToUpper(string(args[1]))
	switch subCommand {
	case "GET":
		return getConfig(args[2:])
	case "SET":
		return setConfig(args[2:])
	case "RESETSTAT":
		// todo add resetstat
		return protocol.MakeErrReply(fmt.Sprintf("Unknown subcommand or wrong number of arguments for '%s'", subCommand))
	case "REWRITE":
		// todo add rewrite
		return protocol.MakeErrReply(fmt.Sprintf("Unknown subcommand or wrong number of arguments for '%s'", subCommand))
	default:
		return protocol.MakeErrReply(fmt.Sprintf("Unknown subcommand or wrong number of arguments for '%s'", subCommand))
	}
}
func getConfig(args [][]byte) redis.Reply {
	result := make([]redis.Reply, 0)
	for _, arg := range args {
		param := string(arg)
		for key, value := range config.PropertiesMap {
			pattern, err := wildcard.CompilePattern(param)
			if err != nil {
				return nil
			}
			isMatch := pattern.IsMatch(key)
			if isMatch {
				result = append(result, protocol.MakeBulkReply([]byte(key)), protocol.MakeBulkReply([]byte(value)))
			}
		}
	}
	return protocol.MakeMultiRawReply(result)
}

func setConfig(args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'config|set' command")
	}
	properties := config.CopyProperties()
	updateMap := make(map[string]string)
	for i := 0; i < len(args); i += 2 {
		parameter := string(args[i])
		value := string(args[i+1])
		if _, ok := updateMap[parameter]; ok {
			errStr := fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - duplicate parameter", parameter)
			return protocol.MakeErrReply(errStr)
		}
		updateMap[parameter] = value
	}
	for parameter, value := range updateMap {
		if _, ok := config.PropertiesMap[parameter]; !ok {
			return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown option or number of arguments for CONFIG SET - '%s'", parameter))
		}
		isImmutable := config.IsImmutableConfig(parameter)
		if !isImmutable {
			return protocol.MakeErrReply(fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - can't set immutable config", parameter))
		}
		err := updateConfig(properties, parameter, value)
		if err != nil {
			return err
		}
	}

	config.Properties = properties
	err := config.UpdatePropertiesMap()
	if err != nil {
		return err
	}
	return &protocol.OkReply{}
}

func updateConfig(properties *config.ServerProperties, parameter string, value string) redis.Reply {
	t := reflect.TypeOf(properties)
	v := reflect.ValueOf(properties)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok || strings.TrimLeft(key, " ") == "" {
			key = field.Name
		}
		if key == parameter {
			switch fieldVal.Type().Kind() {
			case reflect.String:
				fieldVal.SetString(value)
				break
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					errStr := fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - argument couldn't be parsed into an integer", parameter)
					return protocol.MakeErrReply(errStr)
				}
				fieldVal.SetInt(intValue)
				break
			case reflect.Bool:
				if "yes" == value {
					fieldVal.SetBool(true)
				} else if "no" == value {
					fieldVal.SetBool(false)
				} else {
					errStr := fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - argument couldn't be parsed into a bool", parameter)
					return protocol.MakeErrReply(errStr)
				}
				break
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
				break
			}
		}
	}
	return nil
}
