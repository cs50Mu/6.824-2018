package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var err error
	// var oFile *os.File
	oFile, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("open outFile failed, err: %+v", err)
	}
	enc := json.NewEncoder(oFile)
	defer oFile.Close()
	var kvs []KeyValue
	for i := 0; i < nMap; i++ {
		tmpFileName := reduceName(jobName, i, reduceTask)
		file, err := os.Open(tmpFileName)
		if err != nil {
			log.Fatalf("open file failed, err: %+v", err)
		}
		dec := json.NewDecoder(file)
		var kv KeyValue
		for err == nil {
			err = dec.Decode(&kv)
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	// log.Printf("before sort kvs: %+v\n", kvs[:50])
	// sort by key
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	// log.Printf("after sort kvs: %+v\n", kvs[:50])
	var i, j, k int
	for i = 0; i < len(kvs); {
		for j = i + 1; j < len(kvs) && kvs[i].Key == kvs[j].Key; {
			j++
		}
		var vals []string
		for k = i; k < j; k++ {
			vals = append(vals, kvs[k].Value)
		}
		err = enc.Encode(KeyValue{Key: kvs[i].Key, Value: reduceF(kvs[i].Key, vals)})
		if err != nil {
			log.Fatalf("encode json failed, err: %v", err)
		}
		i = j
	}
}
