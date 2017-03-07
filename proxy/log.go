package proxy

type commandlog struct {
	log   []string
	index int
	size  int
}

func NewCommandLog(size int) *commandlog {
	return &commandlog{make([]string, size), 0, size}
}

func (cl *commandlog) Push(msg []byte) {
	cl.log[cl.index] = string(msg)
	cl.index = cl.index + 1
	if cl.index >= cl.size {
		cl.index = 0
	}
}

func (cl *commandlog) Dump() []string {
	var ordered []string
	for i := 0; i < cl.size; i++ {
		val := cl.log[(cl.index+i)%cl.size]
		if "" == val {
			continue
		}
		ordered = append(ordered, val)
	}
	return ordered
}
