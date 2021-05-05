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
