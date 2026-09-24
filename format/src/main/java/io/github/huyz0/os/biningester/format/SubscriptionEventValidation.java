// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

/** Constructor invariants for {@link SubscriptionEvent}. */
final class SubscriptionEventValidation {
    private SubscriptionEventValidation() { }

    static void validate(String session, long sequencerEpoch, long sessionEpoch,
            long firstOffset, int recordCount, FetchMode via, byte[] inline,
            Grant grant, long byteStart, long byteLen, long chainSequence) {
        if (session.isBlank()) {
            throw new IllegalArgumentException("a blank session identifies no subscription");
        }
        if (sequencerEpoch < 0) {
            throw new IllegalArgumentException("an epoch of " + sequencerEpoch + " is not a term");
        }
        if (sessionEpoch < 0) {
            throw new IllegalArgumentException(
                    "a session epoch of " + sessionEpoch + " is not a request order");
        }
        if (chainSequence < SubscriptionEvent.CHAIN_SEQUENCE_ABSENT) {
            throw new IllegalArgumentException("chain sequence is invalid: " + chainSequence);
        }
        if (chainSequence != SubscriptionEvent.CHAIN_SEQUENCE_ABSENT
                && sessionEpoch == SubscriptionEvent.SESSION_EPOCH_ABSENT) {
            throw new IllegalArgumentException("a v4 event needs a session epoch");
        }
        validateGrantAndRange(sessionEpoch, grant, byteStart, byteLen);
        validateOffsets(firstOffset, recordCount);
        validateFetchMode(via, inline);
    }

    private static void validateGrantAndRange(long sessionEpoch, Grant grant,
            long byteStart, long byteLen) {
        // A grant implies a session epoch because the v3 wire shape contains it.
        if (grant != null && sessionEpoch == SubscriptionEvent.SESSION_EPOCH_ABSENT) {
            throw new IllegalArgumentException("a grant needs a session epoch: a v3 body carries "
                    + "one, so an absent epoch cannot be encoded alongside a grant");
        }
        if (grant == null && (byteStart != SubscriptionEvent.RANGE_ABSENT
                || byteLen != SubscriptionEvent.RANGE_ABSENT)) {
            throw new IllegalArgumentException(
                    "a byte range without a grant has nothing to scope; ADR-0041's range is part "
                            + "of the grant, not of the event");
        }
        if ((byteStart == SubscriptionEvent.RANGE_ABSENT)
                != (byteLen == SubscriptionEvent.RANGE_ABSENT)) {
            throw new IllegalArgumentException("a byte range needs both a start and a length, "
                    + "got (" + byteStart + ", " + byteLen + ")");
        }
        if (byteStart != SubscriptionEvent.RANGE_ABSENT && byteLen <= 0) {
            throw new IllegalArgumentException("a byte range of length " + byteLen
                    + " grants nothing");
        }
        if (byteStart != SubscriptionEvent.RANGE_ABSENT && byteStart < 0) {
            throw new IllegalArgumentException(
                    "a byte range starting at " + byteStart + " is not a range");
        }
    }

    private static void validateOffsets(long firstOffset, int recordCount) {
        if (firstOffset < 0) {
            throw new IllegalArgumentException("offsets are never negative");
        }
        if (recordCount <= 0) {
            throw new IllegalArgumentException("a push of nothing is not a push");
        }
        // Subtract first so the legitimate (MAX_VALUE, 1) range remains valid.
        if (firstOffset > Long.MAX_VALUE - (recordCount - 1)) {
            throw new IllegalArgumentException(
                    "a push of " + recordCount + " from " + firstOffset + " runs past the last "
                            + "addressable offset");
        }
    }

    private static void validateFetchMode(FetchMode via, byte[] inline) {
        if (via == FetchMode.INLINE && inline.length == 0) {
            throw new IllegalArgumentException(
                    "via=INLINE carries the bytes, and none are attached");
        }
        if (via != FetchMode.INLINE && inline.length > 0) {
            throw new IllegalArgumentException(
                    "via=" + via + " does not carry bytes, but " + inline.length + " are attached");
        }
    }
}
