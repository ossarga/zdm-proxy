package zdmproxy

import (
	"crypto/md5"
	"fmt"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/require"
)

const MaxPSCacheSizeForTests = 10
const OriginIdPrefix = "originId_"
const TargetIdPrefix = "targetId_"
const InterceptedIdPrefix = "interceptedId_"

type CacheMapType string

const (
	CacheMapTypeOrigin      = CacheMapType("INDEX-ORIGIN")
	CacheMapTypeTarget      = CacheMapType("INDEX-TARGET")
	CacheMapTypeIntercepted = CacheMapType("INTERCEPTED")
	CacheMapTypeNone        = CacheMapType("NONE")
)

/*
*
This test has the sole purpose to verify that all three cache maps ("cache", "index" and "intercepted") honour the configured size limit and behave in an LRU fashion
It does not represent a realistic usage of the PS Cache: elements are added to all three cache maps, which would not normally happen, and the data inserted in it is intentionally dummy
*/
func TestPreparedStatementCache_StoreIntoAllCacheMaps(t *testing.T) {

	tests := []struct {
		name                           string
		numElementsToAdd               int
		elementSuffixesToAccess        []int
		expectedCacheMapSize           int
		expectedElementSuffixesInCache []int
	}{
		{
			name:                           "insert less elements than capacity, nothing accessed, nothing evicted",
			numElementsToAdd:               9,
			elementSuffixesToAccess:        []int{},
			expectedCacheMapSize:           9,
			expectedElementSuffixesInCache: []int{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name:                           "insert as many elements as capacity, nothing accessed, nothing evicted",
			numElementsToAdd:               10,
			elementSuffixesToAccess:        []int{},
			expectedCacheMapSize:           10,
			expectedElementSuffixesInCache: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:                           "insert more elements than capacity, nothing accessed, overflowing oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{},
			expectedCacheMapSize:           MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:                           "insert more elements than capacity, only recent ones accessed, overflowing oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{5, 7, 9},
			expectedCacheMapSize:           MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:                           "insert more elements than capacity, overflowing oldest ones accessed, non-accessed oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{0, 2},
			expectedCacheMapSize:           MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{0, 2, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			name:                           "insert more elements than capacity, overflowing oldest and recent ones accessed, non-accessed oldest ones should be evicted",
			numElementsToAdd:               13,
			elementSuffixesToAccess:        []int{0, 2, 3, 8},
			expectedCacheMapSize:           MaxPSCacheSizeForTests,
			expectedElementSuffixesInCache: []int{0, 2, 3, 6, 7, 8, 9, 10, 11, 12},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			psCache, err := NewPreparedStatementCache(MaxPSCacheSizeForTests)
			require.Nil(tt, err, "Error creating the PSCache", err)

			if test.numElementsToAdd < MaxPSCacheSizeForTests {
				// no overflow or evictions, just insert all elements
				for i := 0; i < test.numElementsToAdd; i++ {
					originPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(OriginIdPrefix, i)),
					}
					targetPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(TargetIdPrefix, i)),
					}
					psCache.StorePreparedOnBoth(originPreparedResult, targetPreparedResult, createDummyPrepareRequestInfo(fmt.Sprintf("query_%d", i)))
				}
			} else {
				// fill the cache
				for i := 0; i < MaxPSCacheSizeForTests; i++ {
					originPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(OriginIdPrefix, i)),
					}
					targetPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(TargetIdPrefix, i)),
					}
					psCache.StorePreparedOnBoth(originPreparedResult, targetPreparedResult, createDummyPrepareRequestInfo(fmt.Sprintf("query_%d", i)))
				}

				// access the specified elements
				for _, elementSuffix := range test.elementSuffixesToAccess {
					// access the specified elements to make them recently used
					originId := []byte(fmt.Sprint(OriginIdPrefix, elementSuffix))
					targetId := []byte(fmt.Sprint(TargetIdPrefix, elementSuffix))
					_, foundInOriginMap := psCache.GetByOriginPreparedId(originId)
					require.True(tt, foundInOriginMap, "element could not be found in origin map", elementSuffix)
					_, foundInTargetMap := psCache.GetByTargetPreparedId(targetId)
					require.True(tt, foundInTargetMap, "element could not be found in target map", elementSuffix)
				}

				// add more elements
				for i := MaxPSCacheSizeForTests; i < test.numElementsToAdd; i++ {
					originPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(OriginIdPrefix, i)),
					}
					targetPreparedResult := &message.PreparedResult{
						PreparedQueryId: []byte(fmt.Sprint(TargetIdPrefix, i)),
					}
					psCache.StorePreparedOnBoth(originPreparedResult, targetPreparedResult, createDummyPrepareRequestInfo(fmt.Sprintf("query_%d", i)))
				}
			}

			require.Equal(tt, test.expectedCacheMapSize, len(psCache.indexOrigin))
			require.Equal(tt, test.expectedCacheMapSize, len(psCache.indexTarget))
			require.Equal(tt, float64(test.expectedCacheMapSize), psCache.GetPreparedStatementCacheSize())

			for _, elementSuffix := range test.expectedElementSuffixesInCache {
				foundInOriginMap := checkIfElementIsInOriginMap(psCache, elementSuffix)
				require.True(tt, foundInOriginMap, "element could not be found in origin map", elementSuffix)
				foundInTargetMap := checkIfElementIsInTargetMap(psCache, elementSuffix)
				require.True(tt, foundInTargetMap, "element could not be found in target map", elementSuffix)
			}

		})
	}

}

func createDummyPrepareRequestInfo(query string) *PrepareRequestInfo {
	return NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false, false), false, []*term{}, false, "", query, "")
}

func checkIfElementIsInOriginMap(psCache *PreparedStatementCache, elementSuffix int) bool {
	originId := fmt.Sprint(OriginIdPrefix, elementSuffix)
	// not using psCache.Get, which is tested separately
	_, foundOriginId := psCache.indexOrigin[originId]
	return foundOriginId
}

func checkIfElementIsInTargetMap(psCache *PreparedStatementCache, elementSuffix int) bool {
	targetId := fmt.Sprint(TargetIdPrefix, elementSuffix)
	// not using psCache.GetByTargetPreparedId, which is tested separately
	_, foundTargetId := psCache.indexTarget[targetId]
	return foundTargetId
}

/*
*
This test focuses on ensuring that Get and GetByTargetPreparedId work correctly.
It inserts elements directly into the cache maps to avoid coupling this test to the logic in the PS Cache's store methods.
It uses dummy, non-realistic data.
*/
func TestPreparedStatementCache_GetFromCache(t *testing.T) {

	tests := []struct {
		name         string
		elementId    string
		cacheMapType CacheMapType
	}{
		{
			name:         "Add to origin cache map, found by GetByOriginPreparedId",
			elementId:    "someOriginId",
			cacheMapType: CacheMapTypeOrigin,
		},
		{
			name:         "Add to target cache map, found by GetByTargetPreparedId",
			elementId:    "someTargetId",
			cacheMapType: CacheMapTypeTarget,
		},
		{
			name:         "Add to intercepted cache map, found by GetByClientPreparedId",
			elementId:    "someInterceptedId",
			cacheMapType: CacheMapTypeIntercepted,
		},
		{
			name:         "Not added, not found",
			elementId:    "someElementId",
			cacheMapType: CacheMapTypeNone,
		},
	}

	dummyPreparedResult := &message.PreparedResult{
		PreparedQueryId: []byte("dummy"),
	}
	dummyPrepareRequestInfo := NewPrepareRequestInfo(NewGenericRequestInfo(forwardToBoth, false, false), false, []*term{}, false, "", "dummyQuery", "")
	dummyPreparedEntry := NewPreparedEntry(
		md5.Sum([]byte("dummyQuery")),
		dummyPrepareRequestInfo,
		NewPreparedData(dummyPreparedResult, dummyPreparedResult),
	)

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			psCache, err := NewPreparedStatementCache(MaxPSCacheSizeForTests)
			require.Nil(tt, err, "Error creating the PSCache", err)

			clientPreparedId := md5.Sum([]byte(test.elementId))

			switch test.cacheMapType {
			case CacheMapTypeOrigin:
				psCache.indexOrigin[test.elementId] = clientPreparedId
				psCache.cache.Add(clientPreparedId, dummyPreparedEntry)

				_, foundByOrigin := psCache.GetByOriginPreparedId([]byte(test.elementId))
				require.True(tt, foundByOrigin)

				_, foundByTarget := psCache.GetByTargetPreparedId([]byte(test.elementId))
				require.False(tt, foundByTarget)
			case CacheMapTypeTarget:
				psCache.indexTarget[test.elementId] = clientPreparedId
				psCache.cache.Add(clientPreparedId, dummyPreparedEntry)

				_, foundByOrigin := psCache.GetByOriginPreparedId([]byte(test.elementId))
				require.False(tt, foundByOrigin)

				_, foundByTarget := psCache.GetByTargetPreparedId([]byte(test.elementId))
				require.True(tt, foundByTarget)
			case CacheMapTypeIntercepted:
				interceptedEntry := NewInterceptedPreparedEntry(clientPreparedId, dummyPrepareRequestInfo)
				psCache.cache.Add(clientPreparedId, interceptedEntry)

				_, foundByClient := psCache.GetByClientPreparedId(clientPreparedId)
				require.True(tt, foundByClient)

				_, foundByOrigin := psCache.GetByOriginPreparedId([]byte(test.elementId))
				require.False(tt, foundByOrigin)

				_, foundByTarget := psCache.GetByTargetPreparedId([]byte(test.elementId))
				require.False(tt, foundByTarget)
			case CacheMapTypeNone:
				_, foundByGet := psCache.GetByClientPreparedId(clientPreparedId)
				require.False(tt, foundByGet)

				_, foundByOrigin := psCache.GetByOriginPreparedId([]byte(test.elementId))
				require.False(tt, foundByOrigin)

				_, foundByTarget := psCache.GetByTargetPreparedId([]byte(test.elementId))
				require.False(tt, foundByTarget)
			default:
				t.Fatal("Unknown or missing cache map type")
			}
		})
	}

}
