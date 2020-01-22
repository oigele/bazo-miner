package protocol

import (
	"bytes"
	"encoding/gob"
	"fmt"
)


//Seperate data type to keep it more expandable
type DataSummary struct {
	Address						[32]byte
	Data						[][]byte
}



func NewDataSummary(address [32]byte) *DataSummary {
	newDataSummary := DataSummary{
		address,
		nil,
	}

	return &newDataSummary
}

//Address is the unique identifier of a data summary
func (ds *DataSummary) Hash() [32]byte {
	if ds == nil {
		return [32]byte{}
	}
	return ds.Address
}



func (ds *DataSummary) Encode() []byte {
	if ds == nil {
		return nil
	}

	encoded := DataSummary{
		Address:					ds.Address,
		Data:						ds.Data,
	}

	buffer := new(bytes.Buffer)
	gob.NewEncoder(buffer).Encode(encoded)
	return buffer.Bytes()
}

func (*DataSummary) Decode(encoded []byte) (ds *DataSummary) {
	var decoded DataSummary
	buffer := bytes.NewBuffer(encoded)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&decoded)
	return &decoded
}

func (ds DataSummary) String() string {
	addressHash := ds.Hash()
	return fmt.Sprintf(
		"Address: %x, " +
			"Data: %x, ",
		addressHash[0:8],
		ds.Data,
		)
}

