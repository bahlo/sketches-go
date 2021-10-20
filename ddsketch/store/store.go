// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package store

import (
	"errors"

	enc "github.com/bahlo/sketches-go/ddsketch/encoding"
)

type Provider func() Store

var (
	DefaultProvider                   = Provider(BufferedPaginatedStoreConstructor)
	DenseStoreConstructor             = Provider(func() Store { return NewDenseStore() })
	BufferedPaginatedStoreConstructor = Provider(func() Store { return NewBufferedPaginatedStore() })
	SparseStoreConstructor            = Provider(func() Store { return NewSparseStore() })
)

const (
	maxInt = int(^uint(0) >> 1)
	minInt = ^maxInt
)

var (
	errUndefinedMinIndex = errors.New("MinIndex of empty store is undefined")
	errUndefinedMaxIndex = errors.New("MaxIndex of empty store is undefined")
)

type Store interface {
	Add(index int)
	AddBin(bin Bin)
	AddWithCount(index int, count float64)
	// Bins returns a channel that emits the bins that are encoded in the store.
	// Note that this leaks a channel and a goroutine if it is not iterated to completion.
	Bins() <-chan Bin
	// ForEach applies f to all elements of the store or until f returns true.
	ForEach(f func(index int, count float64) (stop bool))
	Copy() Store
	// Clear empties the store while allowing reusing already allocated memory.
	// In some situations, it may be advantageous to clear and reuse a store
	// rather than instantiating a new one. Keeping reusing the same store again
	// and again on varying input data distributions may however ultimately make
	// the store overly large and may waste memory space.
	Clear()
	IsEmpty() bool
	MaxIndex() (int, error)
	MinIndex() (int, error)
	TotalCount() float64
	KeyAtRank(rank float64) int
	// Reweight multiplies all values from the store by w, but keeps the same global distribution.
	Reweight(w float64) error
	// Encode encodes the bins of the store and appends its content to the
	// provided []byte.
	// The provided FlagType indicates whether the store encodes positive or
	// negative values.
	Encode(b *[]byte, t enc.FlagType)
	// DecodeAndMergeWith decodes bins that have been encoded in the format of
	// the provided binEncodingMode and merges them within the receiver store.
	// It updates the provided []byte so that it starts immediately after the
	// encoded bins.
	DecodeAndMergeWith(b *[]byte, binEncodingMode enc.SubFlag) error
}

func DecodeAndMergeWith(s Store, b *[]byte, binEncodingMode enc.SubFlag) error {
	switch binEncodingMode {

	case enc.BinEncodingIndexDeltasAndCounts:
		numBins, err := enc.DecodeUvarint64(b)
		if err != nil {
			return err
		}
		index := int64(0)
		for i := uint64(0); i < numBins; i++ {
			indexDelta, err := enc.DecodeVarint64(b)
			if err != nil {
				return err
			}
			count, err := enc.DecodeVarfloat64(b)
			if err != nil {
				return err
			}
			index += indexDelta
			s.AddWithCount(int(index), count)
		}

	case enc.BinEncodingIndexDeltas:
		numBins, err := enc.DecodeUvarint64(b)
		if err != nil {
			return err
		}
		index := int64(0)
		for i := uint64(0); i < numBins; i++ {
			indexDelta, err := enc.DecodeVarint64(b)
			if err != nil {
				return err
			}
			index += indexDelta
			s.Add(int(index))
		}

	case enc.BinEncodingContiguousCounts:
		numBins, err := enc.DecodeUvarint64(b)
		if err != nil {
			return err
		}
		index, err := enc.DecodeVarint64(b)
		if err != nil {
			return err
		}
		indexDelta, err := enc.DecodeVarint64(b)
		if err != nil {
			return err
		}
		for i := uint64(0); i < numBins; i++ {
			count, err := enc.DecodeVarfloat64(b)
			if err != nil {
				return err
			}
			s.AddWithCount(int(index), count)
			index += indexDelta
		}

	default:
		return errors.New("unknown bin encoding")
	}
	return nil
}
