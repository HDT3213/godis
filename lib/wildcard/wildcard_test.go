package wildcard

import "testing"

func TestWildCard(t *testing.T) {
	p := CompilePattern("")
	if !p.IsMatch("") {
		t.Error("expect true actually false")
	}
	p = CompilePattern("a")
	if !p.IsMatch("a") {
		t.Error("expect true actually false")
	}
	if p.IsMatch("b") {
		t.Error("expect false actually true")
	}

	// test '?'
	p = CompilePattern("a?")
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
	p = CompilePattern("a*")
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
	p = CompilePattern("a[ab[]")
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
	p = CompilePattern("h[a-c]llo")
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

	// test [^]
	p = CompilePattern("h[^ab]llo")
	if p.IsMatch("hallo") {
		t.Error("expect false actually true")
	}
	if p.IsMatch("hbllo") {
		t.Error("expect false actually true")
	}
	if !p.IsMatch("hcllo") {
		t.Error("expect true actually false")
	}

	// test escape
	p = CompilePattern("\\\\") // pattern: \\
	if !p.IsMatch("\\") {
		t.Error("expect true actually false")
	}

	p = CompilePattern("\\*")
	if !p.IsMatch("*") {
		t.Error("expect true actually false")
	}
	if p.IsMatch("a") {
		t.Error("expect false actually true")
	}
}
