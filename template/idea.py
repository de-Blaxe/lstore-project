def finalize_schema(self, page_range, page_row, byte_pos, need_baseSet):
    # need_baseSet is a Boolean
    page_set = page_range.base_set if need_baseSet else page_range.tail_set
    schema_data = page_set[page_row][SCHEMA_ENCODING_COLUMN].data
    init_schema = str(int.from_bytes(schema_data[byte_pos:byte_pos + DATA_SIZE], 'little'))

    diff = self.num_columns - len(init_schema)
    final_schema = ('0' * diff) + init_schema if diff else init_schema
    return [final_schema, diff] # diff used in merge()


# or

def pad_schema2(self, rid):
    [page_range_index, page_row, byte_pos, _] = self.page_directory[rid]
    #page_range = 
    pass
