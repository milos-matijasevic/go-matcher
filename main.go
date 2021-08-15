package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

const (
	jsonTag = "json"
)

// Matcher e
type Matcher struct {
	MissingFields []string
	currentFields []string
}

// JSONEqual compares a given struct to a given json byte array
func (m *Matcher) JSONEqual(s interface{}, j []byte) bool {
	v := reflect.ValueOf(s)

	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		panic("not struct")
	}

	var jsonMap map[string]interface{}
	err := json.Unmarshal(j, &jsonMap)
	if err != nil {
		panic(err)
	}

	m.MissingFields = nil
	m.currentFields = nil
	return m.checkStruct(jsonMap, v.Type())
}

func (m *Matcher) createFieldAccessString() string {
	var sb strings.Builder
	for _, str := range m.currentFields {
		if str[0] != '[' {
			sb.WriteString(".")
		}
		sb.WriteString(str)
	}
	return sb.String()
}

func (m *Matcher) checkStruct(s map[string]interface{}, t reflect.Type) bool {
	ret := true
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Unexported field
		if field.PkgPath != "" {
			continue
		}

		jsonName := field.Tag.Get(jsonTag)
		if jsonName == "-" {
			continue
		}

		jsonValue, ok := s[jsonName]
		if !ok {
			m.MissingFields = append(m.MissingFields, fmt.Sprintf("%s.%s", m.createFieldAccessString(), jsonName))
			ret = false
		}
		m.currentFields = append(m.currentFields, jsonName)
		ret = m.checkValue(jsonValue, field.Type) && ret
		m.currentFields = m.currentFields[:len(m.currentFields)-1]
	}
	return ret
}

func (m *Matcher) checkValue(v interface{}, t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		jStruct, ok := v.(map[string]interface{})
		if !ok {
			m.MissingFields = append(m.MissingFields, fmt.Sprintf("%s", m.createFieldAccessString()))
			return false
		}
		return m.checkStruct(jStruct, t)

	case reflect.Slice, reflect.Array:
		jSlice, ok := v.([]interface{})
		if !ok {
			m.MissingFields = append(m.MissingFields, fmt.Sprintf("%s", m.createFieldAccessString()))
			return false
		}
		return m.checkSlice(jSlice, t.Elem())

	case reflect.Map:
		jMap, ok := v.(map[string]interface{})
		if !ok {
			m.MissingFields = append(m.MissingFields, fmt.Sprintf("%s", m.createFieldAccessString()))
			return false
		}
		return m.checkMap(jMap, t.Elem())
	}
	return true
}

func (m *Matcher) checkSlice(jSlice []interface{}, t reflect.Type) bool {
	ret := true
	for i, v := range jSlice {
		m.currentFields = append(m.currentFields, fmt.Sprintf("[%d]", i))
		ret = m.checkValue(v, t) && ret
		m.currentFields = m.currentFields[:len(m.currentFields)-1]
	}
	return ret
}

func (m *Matcher) checkMap(jMap map[string]interface{}, t reflect.Type) bool {
	ret := true
	for k, v := range jMap {
		m.currentFields = append(m.currentFields, fmt.Sprintf("[\"%s\"]", k))
		ret = m.checkValue(v, t) && ret
		m.currentFields = m.currentFields[:len(m.currentFields)-1]
	}
	return ret
}

func main() {
	input, err := ioutil.ReadFile("input")
	if err != nil {
		panic(err)
	}

	matcher := Matcher{}
	fmt.Println(matcher.JSONEqual(&utils.PersistentTopicInternalStats{}, input))
	fmt.Println(strings.Join(matcher.MissingFields, "\n"))
}
