package config

import (
	"bufio"
	"github.com/hdt3213/godis/lib/logger"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const DefaultConfPath = "redis.conf"

// Properties holds global config properties
var Properties *ServerProperties

// ServerProperties defines global config properties
type ServerProperties struct {
	Bind           string `cfg:"bind"`
	Port           int    `cfg:"port"`
	AppendOnly     bool   `cfg:"appendOnly"`
	AppendFilename string `cfg:"appendFilename"`
	MaxClients     int    `cfg:"maxclients"`
	RequirePass    string `cfg:"requirepass"`

	Peers []string `cfg:"peers"`
	Self  string   `cfg:"self"`
}

func init() {
	// default config
	Properties = &ServerProperties{
		Bind:           "0.0.0.0",
		Port:           6399,
		AppendOnly:     false,
		AppendFilename: "",
		MaxClients:     1000,
	}
}

func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				fieldVal.SetBool(toBool(value))
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}
	return config
}

// Setup read config file and store properties into Properties
func Setup(configFilename string) {
	if configFilename == "" {
		if defaultConfigFileExists() {
			configFilename = DefaultConfPath
		}
	}
	file, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Properties = parse(file)
}

func defaultConfigFileExists() bool {
	info, err := os.Stat(DefaultConfPath)
	return err == nil && !info.IsDir()
}

func toBool(s string) bool {
	ls := strings.ToLower(s)
	switch ls {
	case "true", "yes", "t", "y":
		return true
	default:
		return false
	}
}