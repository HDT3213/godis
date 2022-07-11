package wildcard

import "testing"

func TestWildCard(t *testing.T) {
	p, err := CompilePattern("")
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("") {
		t.Error("expect true actually false")
	}
	p, err = CompilePattern("a")
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("a") {
		t.Error("expect true actually false")
	}
	if p.IsMatch("b") {
		t.Error("expect false actually true")
	}

	// test '?'
	p, err = CompilePattern("a?")
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("ab") {
		t.Error("expect true actually false")
	}
	if p.IsMatch("a") {
		t.Error("expect false actually true")
	}
	if p.IsMatch("abb") {
		t.Error("expect false actually true")
	}
	if p.IsMatch("bb") {
		t.Error("expect false actually true")
	}

	// test *
	p, err = CompilePattern("a*")
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("ab") {
		t.Error("expect true actually false")
	}
	if !p.IsMatch("a") {
		t.Error("expect true actually false")
	}
	if !p.IsMatch("abb") {
		t.Error("expect true actually false")
	}
	if p.IsMatch("bb") {
		t.Error("expect false actually true")
	}

	// test []
	p, err = CompilePattern("a[ab[]")
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("ab") {
		t.Error("expect true actually false")
	}
	if !p.IsMatch("aa") {
		t.Error("expect true actually false")
	}
	if !p.IsMatch("a[") {
		t.Error("expect true actually false")
	}
	if p.IsMatch("abb") {
		t.Error("expect false actually true")
	}
	if p.IsMatch("bb") {
		t.Error("expect false actually true")
	}

	// test [a-c]
	p, err = CompilePattern("h[a-c]llo")
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("hallo") {
		t.Error("expect true actually false")
	}
	if !p.IsMatch("hbllo") {
		t.Error("expect true actually false")
	}
	if !p.IsMatch("hcllo") {
		t.Error("expect true actually false")
	}
	if p.IsMatch("hdllo") {
		t.Error("expect false actually true")
	}
	if p.IsMatch("hello") {
		t.Error("expect false actually true")
	}

	//test [^]
	p, err = CompilePattern("h[^ab]llo")
	if err != nil {
		t.Error(err)
		return
	}
	if p.IsMatch("hallo") {
		t.Error("expect false actually true")
	}
	if p.IsMatch("hbllo") {
		t.Error("expect false actually true")
	}
	if !p.IsMatch("hcllo") {
		t.Error("expect true actually false")
	}

	p, err = CompilePattern("[^ab]c")
	if err != nil {
		t.Error(err)
		return
	}
	if p.IsMatch("abc") {
		t.Error("expect false actually true")
	}
	if !p.IsMatch("1c") {
		t.Error("expect true actually false")
	}

	p, err = CompilePattern("1^2")
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("1^2") {
		t.Error("expect true actually false")
	}

	p, err = CompilePattern(`\[^1]2`)
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("[^1]2") {
		t.Error("expect true actually false")
	}

	p, err = CompilePattern(`^1`)
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("^1") {
		t.Error("expect true actually false")
	}

	// test escape
	p, err = CompilePattern(`\\\\`)
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch(`\\`) {
		t.Error("expect true actually false")
	}

	p, err = CompilePattern("\\*")
	if err != nil {
		t.Error(err)
		return
	}
	if !p.IsMatch("*") {
		t.Error("expect true actually false")
	}
	if p.IsMatch("a") {
		t.Error("expect false actually true")
	}

	p, err = CompilePattern(`\`)
	if err == nil || err.Error() != errEndWithEscape {
		t.Error(err)
		return
	}
}
