brew install protobuf

protoc -I=$SRC_DIR --csharp_out=$DST_DIR $SRC_DIR/order_event.proto