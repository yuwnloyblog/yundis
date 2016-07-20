package utils

import(
	"strings"
	"encoding/json"
)
/**
 * parse json string to struct
 */
func JsonParse(jsonStr string, v interface{})error{
	dec := json.NewDecoder(strings.NewReader(jsonStr))
	err := dec.Decode(v);
	if err != nil{
		return err
	}
	return nil
}
/**
 * convert struct to json string
 */
func ToJson(v interface{})(string,error){
	bytes,err:=json.Marshal(v)
	if err != nil {
		return "",err
	}else{
		return string(bytes),nil
	}
}