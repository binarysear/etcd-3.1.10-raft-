// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "etcd_test/raft/raftpb"

//import pb "github.com/coreos/etcd/raft/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.

type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	// 还未持久化的预写日志
	entries []pb.Entry
	// 用于保存预写日志数组中的还未持久化的预写日志的起始索引
	// *保存快照和日志数组的分界线  感觉是stable和unstable的分界线
	offset uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot == nil {
			return 0, false
		}
		if u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}
	return u.entries[i-u.offset].Term, true
}

// 将 unstable log 中的某些已经持久化的日志标记为已经稳定并且从 unstable log 中移除这些日志。
// 具体来说，它接收一个索引 i 和一个任期 t，它会从 unstable log 中查找索引为 i 的日志条目，如果该日志条目存在，则它会将该日志条目和之前的所有条目都标记为已稳定，从而将这些日志从 unstable log 中删除。
// 如果在此期间有新的日志条目被添加到 unstable log 中，则这些新的日志条目可以通过 nextEntries 方法被返回，直到下一次调用 acceptInProgress 方法。
// 如果在匹配日志条目的任期时发生任期不匹配、日志缺失或索引超出 unstable log 范围的情况，则函数会忽略这些日志条目，并打印相关的日志信息。
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i >= u.offset {
		u.entries = u.entries[i+1-u.offset:]
		u.offset = i + 1
	}
}

func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

// 判断要添加预写日志是否需要回滚
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	// 先获取要添加的预写日志的第一条数据的索引
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		// 如果索引是下一条要添加数据的索引，直接添加  简单地说就是要添加的日志就是下一条添加的位置就直接添加
		// u.offset+uint64(len(u.entries)) 获取日志的最后一条数据索引+1也就是新增数据的索引
		// after is the next index in the u.entries
		// directly append
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		// 如果要添加的第一条数据索引小于等于偏移量（证明之前添加到raftStorage的部分日志是脏数据），需要回滚全部unstable日志，替换新的偏移量offset和entries
		// 简单地说就是要添加的日志在offset之前，直接丢弃offset后面的enties脏数据
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after
		u.entries = ents
	default:
		// truncate to after and copy to u.entries
		// then append
		// 到此处 u.offset < after < u.offset+uint(len(u.entries)) 也就是要回滚部分日志，重新拼接u.entries
		// 简单来说就是要添加的日志在unstable的中间，也就是有一部分数据是脏数据需要剔除
		u.logger.Infof("truncate the unstable entries before index %d", after)
		// 前面相同部分
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		// 替换需要回滚的后面那部分日志
		u.entries = append(u.entries, ents...)
	}
}

func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.offset)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
