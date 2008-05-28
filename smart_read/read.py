def seek_open(file_name, start=0, end=-1):
    f = open(file_name, 'r')
    
    if start > 0:
        f.seek(start)
    
    length = start
    for line in f:
        length += len(line)
        yield line
        if end > -1 and length > end:
            return
