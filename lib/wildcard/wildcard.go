package wildcard

const (
	normal    = iota
	all       // *
	any       // ?
	setSymbol // []
)

type item struct {
	character byte
	set       map[byte]bool
	typeCode  int
}

func (i *item) contains(c byte) bool {
	_, ok := i.set[c]
	return ok
}

// Pattern represents a wildcard pattern
type Pattern struct {
	items []*item
}

// CompilePattern convert wildcard string to Pattern
func CompilePattern(src string) *Pattern {
	items := make([]*item, 0)
	escape := false
	inSet := false
	var set map[byte]bool
	for _, v := range src {
		c := byte(v)
		if escape {
			items = append(items, &item{typeCode: normal, character: c})
			escape = false
		} else if c == '*' {
			items = append(items, &item{typeCode: all})
		} else if c == '?' {
			items = append(items, &item{typeCode: any})
		} else if c == '\\' {
			escape = true
		} else if c == '[' {
			if !inSet {
				inSet = true
				set = make(map[byte]bool)
			} else {
				set[c] = true
			}
		} else if c == ']' {
			if inSet {
				inSet = false
				items = append(items, &item{typeCode: setSymbol, set: set})
			} else {
				items = append(items, &item{typeCode: normal, character: c})
			}
		} else {
			if inSet {
				set[c] = true
			} else {
				items = append(items, &item{typeCode: normal, character: c})
			}
		}
	}
	return &Pattern{
		items: items,
	}
}

// IsMatch returns whether the given string matches pattern
func (p *Pattern) IsMatch(s string) bool {
	if len(p.items) == 0 {
		return len(s) == 0
	}
	m := len(s)
	n := len(p.items)
	table := make([][]bool, m+1)
	for i := 0; i < m+1; i++ {
		table[i] = make([]bool, n+1)
	}
	table[0][0] = true
	for j := 1; j < n+1; j++ {
		table[0][j] = table[0][j-1] && p.items[j-1].typeCode == all
	}
	for i := 1; i < m+1; i++ {
		for j := 1; j < n+1; j++ {
			if p.items[j-1].typeCode == all {
				table[i][j] = table[i-1][j] || table[i][j-1]
			} else {
				table[i][j] = table[i-1][j-1] &&
					(p.items[j-1].typeCode == any ||
						(p.items[j-1].typeCode == normal && uint8(s[i-1]) == p.items[j-1].character) ||
						(p.items[j-1].typeCode == setSymbol && p.items[j-1].contains(s[i-1])))
			}
		}
	}
	return table[m][n]
}
