// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package mapping

import (
	"errors"

	enc "github.com/bahlo/sketches-go/ddsketch/encoding"
)

const (
	expOverflow      = 7.094361393031e+02      // The value at which math.Exp overflows
	minNormalFloat64 = 2.2250738585072014e-308 //2^(-1022)
)

type IndexMapping interface {
	Equals(other IndexMapping) bool
	Index(value float64) int
	Value(index int) float64
	LowerBound(index int) float64
	RelativeAccuracy() float64
	MinIndexableValue() float64
	MaxIndexableValue() float64
}

func NewDefaultMapping(relativeAccuracy float64) (IndexMapping, error) {
	return NewLogarithmicMapping(relativeAccuracy)
}

// Decode decodes a mapping and updates the provided []byte so that it starts
// immediately after the encoded mapping.
func Decode(b *[]byte, flag enc.Flag) (IndexMapping, error) {
	switch flag {

	case enc.FlagIndexMappingBaseLogarithmic:
		gamma, indexOffset, err := decodeLogLikeIndexMapping(b)
		if err != nil {
			return nil, err
		}
		return NewLogarithmicMappingWithGamma(gamma, indexOffset)

	case enc.FlagIndexMappingBaseLinear:
		gamma, indexOffset, err := decodeLogLikeIndexMapping(b)
		if err != nil {
			return nil, err
		}
		return NewLinearlyInterpolatedMappingWithGamma(gamma, indexOffset)

	case enc.FlagIndexMappingBaseCubic:
		gamma, indexOffset, err := decodeLogLikeIndexMapping(b)
		if err != nil {
			return nil, err
		}
		return NewCubicallyInterpolatedMappingWithGamma(gamma, indexOffset)

	default:
		return nil, errors.New("unknown mapping")
	}
}

func decodeLogLikeIndexMapping(b *[]byte) (gamma, indexOffset float64, err error) {
	gamma, err = enc.DecodeFloat64LE(b)
	if err != nil {
		return
	}
	indexOffset, err = enc.DecodeFloat64LE(b)
	return
}
