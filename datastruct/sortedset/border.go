package sortedset

import (
	"errors"
	"strconv"
)

/*
 * ScoreBorder is a struct represents `min` `max` parameter of redis command `ZRANGEBYSCORE`
 * can accept:
 *   int or float value, such as 2.718, 2, -2.718, -2 ...
 *   exclusive int or float value, such as (2.718, (2, (-2.718, (-2 ...
 *   infinity: +inf, -inf， inf(same as +inf)
 */

const (
	scoreNegativeInf int8 = -1
	scorePositiveInf int8 = 1
	lexNegativeInf   int8 = '-'
	lexPositiveInf   int8 = '+'
)

type Border interface {
	greater(element *Element) bool
	less(element *Element) bool
	getValue() interface{}
	getExclude() bool
	isIntersected(max Border) bool
}

// ScoreBorder represents range of a float value, including: <, <=, >, >=, +inf, -inf
type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

// if max.greater(score) then the score is within the upper border
// do not use min.greater()
func (border *ScoreBorder) greater(element *Element) bool {
	value := element.Score
	if border.Inf == scoreNegativeInf {
		return false
	} else if border.Inf == scorePositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

func (border *ScoreBorder) less(element *Element) bool {
	value := element.Score
	if border.Inf == scoreNegativeInf {
		return true
	} else if border.Inf == scorePositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

func (border *ScoreBorder) getValue() interface{} {
	return border.Value
}

func (border *ScoreBorder) getExclude() bool {
	return border.Exclude
}

var scorePositiveInfBorder = &ScoreBorder{
	Inf: scorePositiveInf,
}

var scoreNegativeInfBorder = &ScoreBorder{
	Inf: scoreNegativeInf,
}

// ParseScoreBorder creates ScoreBorder from redis arguments
func ParseScoreBorder(s string) (Border, error) {
	if s == "inf" || s == "+inf" {
		return scorePositiveInfBorder, nil
	}
	if s == "-inf" {
		return scoreNegativeInfBorder, nil
	}
	if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("ERR min or max is not a float")
		}
		return &ScoreBorder{
			Inf:     0,
			Value:   value,
			Exclude: true,
		}, nil
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, errors.New("ERR min or max is not a float")
	}
	return &ScoreBorder{
		Inf:     0,
		Value:   value,
		Exclude: false,
	}, nil
}

func (border *ScoreBorder) isIntersected(max Border) bool {
	minValue := border.Value
	maxValue := max.(*ScoreBorder).Value
	return minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}

// LexBorder represents range of a string value, including: <, <=, >, >=, +, -
type LexBorder struct {
	Inf     int8
	Value   string
	Exclude bool
}

// if max.greater(lex) then the lex is within the upper border
// do not use min.greater()
func (border *LexBorder) greater(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return false
	} else if border.Inf == lexPositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

func (border *LexBorder) less(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return true
	} else if border.Inf == lexPositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

func (border *LexBorder) getValue() interface{} {
	return border.Value
}

func (border *LexBorder) getExclude() bool {
	return border.Exclude
}

var lexPositiveInfBorder = &LexBorder{
	Inf: lexPositiveInf,
}

var lexNegativeInfBorder = &LexBorder{
	Inf: lexNegativeInf,
}

// ParseLexBorder creates LexBorder from redis arguments
func ParseLexBorder(s string) (Border, error) {
	if s == "+" {
		return lexPositiveInfBorder, nil
	}
	if s == "-" {
		return lexNegativeInfBorder, nil
	}
	if s[0] == '(' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: true,
		}, nil
	}

	if s[0] == '[' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: false,
		}, nil
	}

	return nil, errors.New("ERR min or max not valid string range item")
}

func (border *LexBorder) isIntersected(max Border) bool {
	minValue := border.Value
	maxValue := max.(*LexBorder).Value
	return border.Inf == '+' || minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}
