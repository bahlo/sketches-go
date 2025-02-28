// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package mapping

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testMaxRelativeAccuracy      = 1 - 1e-3
	testMinRelativeAccuracy      = 1e-7
	floatingPointAcceptableError = 1e-12
)

var multiplier = 1 + math.Sqrt(2)*1e2

func TestLogarithmicMappingEquivalence(t *testing.T) {
	relativeAccuracy := 0.01
	gamma := (1 + relativeAccuracy) / (1 - relativeAccuracy)
	mapping1, _ := NewLogarithmicMapping(relativeAccuracy)
	mapping2, _ := NewLogarithmicMappingWithGamma(gamma, 0)
	assert.True(t, mapping1.Equals(mapping2))
}

func TestLinearlyInterpolatedMappingEquivalence(t *testing.T) {
	gamma := 1.6
	relativeAccuracy := 1 - 2/(1+math.Exp(math.Log2(gamma)))
	mapping1, _ := NewLinearlyInterpolatedMapping(relativeAccuracy)
	mapping2, _ := NewLinearlyInterpolatedMappingWithGamma(gamma, 1/math.Log2(gamma))
	assert.True(t, mapping1.Equals(mapping2))
}

func TestCubicallyInterpolatedMappingEquivalence(t *testing.T) {
	gamma := 1.6
	relativeAccuracy := 1 - 2/(1+math.Exp(7.0/10*math.Log2(gamma)))
	mapping1, _ := NewCubicallyInterpolatedMapping(relativeAccuracy)
	mapping2, _ := NewCubicallyInterpolatedMappingWithGamma(gamma, 0)
	assert.True(t, mapping1.Equals(mapping2))
}

func EvaluateRelativeAccuracy(t *testing.T, expected, actual, relativeAccuracy float64) {
	assert.True(t, expected >= 0)
	assert.True(t, actual >= 0)
	if expected == 0 {
		assert.InDelta(t, actual, 0, floatingPointAcceptableError)
	} else {
		assert.True(t, math.Abs(expected-actual)/expected <= relativeAccuracy+floatingPointAcceptableError)
	}
}

func EvaluateMappingAccuracy(t *testing.T, mapping IndexMapping, relativeAccuracy float64) {
	for value := mapping.MinIndexableValue(); value < mapping.MaxIndexableValue(); value *= multiplier {
		mappedValue := mapping.Value(mapping.Index(value))
		EvaluateRelativeAccuracy(t, value, mappedValue, relativeAccuracy)
	}
	value := mapping.MaxIndexableValue()
	mappedValue := mapping.Value(mapping.Index(value))
	EvaluateRelativeAccuracy(t, value, mappedValue, relativeAccuracy)
}

func TestLogarithmicMappingAccuracy(t *testing.T) {
	for relativeAccuracy := testMaxRelativeAccuracy; relativeAccuracy >= testMinRelativeAccuracy; relativeAccuracy *= (testMaxRelativeAccuracy * testMaxRelativeAccuracy) {
		mapping, _ := NewLogarithmicMapping(relativeAccuracy)
		EvaluateMappingAccuracy(t, mapping, relativeAccuracy)
	}
}

func TestLinearlyInterpolatedMappingAccuracy(t *testing.T) {
	for relativeAccuracy := testMaxRelativeAccuracy; relativeAccuracy >= testMinRelativeAccuracy; relativeAccuracy *= (testMaxRelativeAccuracy * testMaxRelativeAccuracy) {
		mapping, _ := NewLinearlyInterpolatedMapping(relativeAccuracy)
		EvaluateMappingAccuracy(t, mapping, relativeAccuracy)
	}
}

func TestCubicallyInterpolatedMappingAccuracy(t *testing.T) {
	for relativeAccuracy := testMaxRelativeAccuracy; relativeAccuracy >= testMinRelativeAccuracy; relativeAccuracy *= (testMaxRelativeAccuracy * testMaxRelativeAccuracy) {
		mapping, _ := NewCubicallyInterpolatedMapping(relativeAccuracy)
		EvaluateMappingAccuracy(t, mapping, relativeAccuracy)
	}
}

func TestLowerBound(t *testing.T) {
	testIndexes := []int{2, 10, 25, 100, 10000}
	logMapping, _ := NewLogarithmicMapping(0.01)
	linearMapping, _ := NewLinearlyInterpolatedMapping(0.01)
	cubicalMapping, _ := NewCubicallyInterpolatedMapping(0.01)
	for _, mapping := range []IndexMapping{logMapping, linearMapping, cubicalMapping} {
		for _, i := range testIndexes {
			lowerBound := mapping.LowerBound(i)
			previous := mapping.Value(i - 1)
			next := mapping.Value(i)
			assert.GreaterOrEqual(t, lowerBound, previous)
			assert.GreaterOrEqual(t, next, lowerBound)
		}
	}
}
