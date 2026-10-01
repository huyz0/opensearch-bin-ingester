// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * A fast-mode term's roster, {@code <prefix>/ctl/fast/0/<epoch %016x>.roster}
 * (ADR-0081 §1, ADR-0082 §3): who can hold the term's copies, the
 * {@code wal_quorum} values it assigned under, the streams it decided, when
 * its leader could first assign, and whether a successor fenced or closed it.
 *
 * <p>⚠️ CANONICAL JSON, one form only (fixed field order, no whitespace, no
 * escapes), decoded strictly: anything but the form {@link #encode} writes is
 * refused, so a roster a reader cannot place is an {@link IOException}, never
 * a differently-read term. Every write of it is a {@code putIfMatch}; the
 * object holds the term's fencing, and two readings of one roster would be
 * two terms' worth of disagreement about who may hold what.
 *
 * <p>⚠️ {@code notBefore} 0 means NO WAIT -- a wall-clock instant in
 * milliseconds is never 0 in a deployment -- and {@code fencedBy} 0 means
 * unfenced: epoch 0 is never a lease's (ADR-0002).
 *
 * @param epoch the term, at least 1
 * @param predecessor the newest earlier roster, or -1 for the first term
 * @param leader the term's leader, also among {@code members}
 * @param members every incarnation that joined, each rostered or departed
 * @param termRecord the {@code wal_quorum} in force at the term's start, then
 *     each coalesced change, numbered from 0 without gaps
 * @param decisions the streams this term decided, by number, ascending; pruned
 *     elements keep their numbers, which are never reused
 * @param notBefore the wall-clock instant (ms) before which the leader assigned
 *     and decided nothing, or 0
 * @param fencedBy the successor term that fenced this one, or 0
 * @param closed whether every stream of this term is decided and committed and
 *     every earlier term is closed
 */
public record Roster(long epoch, long predecessor, Incarnation leader, List<Member> members,
        List<TermRecord> termRecord, List<Decision> decisions, long notBefore, long fencedBy,
        boolean closed) {

    /** The roster's object key under {@code prefix}. */
    public static String key(String prefix, long epoch) {
        return String.format(Locale.ROOT, "%s/ctl/fast/0/%016x.roster", prefix, epoch);
    }

    /** The {@code LATEST} pointer's object key under {@code prefix}. */
    public static String latestKey(String prefix) {
        return prefix + "/ctl/fast/0/LATEST";
    }

    /** A pod's incarnation: its name, its immutable UID, its zone and endpoint. */
    public record Incarnation(String podId, String podUid, String az, String endpoint) {
        public Incarnation {
            Objects.requireNonNull(podId, "podId");
            Objects.requireNonNull(podUid, "podUid");
            Objects.requireNonNull(az, "az");
            Objects.requireNonNull(endpoint, "endpoint");
            if (podId.isBlank() || podUid.isBlank() || az.isBlank()) {
                throw new IllegalArgumentException("an incarnation names its pod, UID and AZ");
            }
            JsonCursor.representable("podId", podId);
            JsonCursor.representable("podUid", podUid);
            JsonCursor.representable("az", az);
            JsonCursor.representable("endpoint", endpoint);
        }
    }

    /** Whether a member can still hold the term's copies. */
    public enum State { ROSTERED, DEPARTED }

    public record Member(Incarnation incarnation, State state) {
        public Member {
            Objects.requireNonNull(incarnation, "incarnation");
            Objects.requireNonNull(state, "state");
        }
    }

    /**
     * One element of the term record: index UUID to {@code wal_quorum}, 1 to 3.
     *
     * <p>⚠️ ORDERED BY THE UUID'S TEXT, not by {@link UUID#compareTo}, which
     * compares signed halves: a canonical form another language's encoder can
     * reproduce sorts the keys it writes.
     */
    public record TermRecord(int seq, SortedMap<UUID, Integer> walQuorum) {
        public TermRecord {
            Objects.requireNonNull(walQuorum, "walQuorum");
            SortedMap<UUID, Integer> byText = byText();
            byText.putAll(walQuorum);
            walQuorum = byText;
            for (Map.Entry<UUID, Integer> e : walQuorum.entrySet()) {
                Objects.requireNonNull(e.getKey(), "index");
                if (e.getValue() == null || e.getValue() < 1 || e.getValue() > 3) {
                    throw new IllegalArgumentException("wal_quorum is 1 to 3: " + e);
                }
            }
            walQuorum = java.util.Collections.unmodifiableSortedMap(walQuorum);
        }

        static SortedMap<UUID, Integer> byText() {
            return new TreeMap<>(java.util.Comparator.comparing(UUID::toString));
        }
    }

    /** A decided stream and the offset its assignment resumes at. */
    public record Decision(int seq, RunKey stream, long resumeAt) {
        public Decision {
            Objects.requireNonNull(stream, "stream");
            if (seq < 0 || resumeAt < 0) {
                throw new IllegalArgumentException("a decision's number and resume offset are "
                        + "never negative");
            }
        }
    }

    public Roster {
        Objects.requireNonNull(leader, "leader");
        members = List.copyOf(members);
        termRecord = List.copyOf(termRecord);
        decisions = List.copyOf(decisions);
        if (epoch < 1) {
            throw new IllegalArgumentException("a roster's epoch is a lease's, at least 1: " + epoch);
        }
        if (predecessor != -1 && (predecessor < 1 || predecessor >= epoch)) {
            throw new IllegalArgumentException("a predecessor is an earlier term or -1: "
                    + predecessor);
        }
        if (notBefore < 0) {
            throw new IllegalArgumentException("notBefore is never negative: " + notBefore);
        }
        if (fencedBy != 0 && fencedBy <= epoch) {
            throw new IllegalArgumentException("only a later term fences: " + fencedBy);
        }
        Set<String> uids = new HashSet<>();
        boolean leaderListed = false;
        for (Member m : members) {
            if (!uids.add(m.incarnation().podUid())) {
                throw new IllegalArgumentException("an incarnation is listed once: "
                        + m.incarnation().podUid());
            }
            leaderListed |= m.incarnation().equals(leader);
        }
        if (!leaderListed) {
            // ⚠️ THE LEADER IS A MEMBER FROM THE ROSTER'S CREATION (ADR-0081
            // §1): its copies count toward every quorum, and a leader outside
            // its own roster was one of the defects M13.41's review found.
            throw new IllegalArgumentException("the leader is among the members");
        }
        for (int i = 0; i < termRecord.size(); i++) {
            if (termRecord.get(i).seq() != i) {
                throw new IllegalArgumentException("the term record is numbered from 0 without "
                        + "gaps");
            }
        }
        for (int i = 1; i < decisions.size(); i++) {
            if (decisions.get(i).seq() <= decisions.get(i - 1).seq()) {
                throw new IllegalArgumentException("decisions are numbered in ascending order");
            }
        }
    }

    /** The member for {@code podUid}, if it joined. */
    public java.util.Optional<Member> member(String podUid) {
        return members.stream().filter(m -> m.incarnation().podUid().equals(podUid)).findFirst();
    }

    /** Whether this term's leader marked itself departed. */
    public boolean leaderDeparted() {
        return member(leader.podUid()).orElseThrow().state() == State.DEPARTED;
    }

    /** The same roster, fenced by {@code successor}. */
    public Roster fencedBy(long successor) {
        return new Roster(epoch, predecessor, leader, members, termRecord, decisions, notBefore,
                successor, closed);
    }

    /** The same roster under another predecessor and {@code notBefore}. */
    public Roster after(long newPredecessor, long newNotBefore) {
        return new Roster(epoch, newPredecessor, leader, members, termRecord, decisions,
                newNotBefore, fencedBy, closed);
    }

    public byte[] encode() {
        StringBuilder out = new StringBuilder();
        out.append("{\"epoch\":").append(epoch).append(",\"predecessor\":").append(predecessor)
                .append(",\"leader\":");
        incarnation(out, leader);
        out.append(",\"members\":[");
        for (int i = 0; i < members.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            Member m = members.get(i);
            incarnation(out, m.incarnation());
            out.setLength(out.length() - 1);
            out.append(",\"state\":\"").append(m.state().name()).append("\"}");
        }
        out.append("],\"termRecord\":[");
        for (int i = 0; i < termRecord.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            TermRecord t = termRecord.get(i);
            out.append("{\"seq\":").append(t.seq()).append(",\"walQuorum\":{");
            boolean first = true;
            for (Map.Entry<UUID, Integer> e : t.walQuorum().entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                out.append('"').append(e.getKey()).append("\":").append(e.getValue());
            }
            out.append("}}");
        }
        out.append("],\"decisions\":[");
        for (int i = 0; i < decisions.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            Decision d = decisions.get(i);
            out.append("{\"seq\":").append(d.seq()).append(",\"index\":\"")
                    .append(d.stream().indexId()).append("\",\"partition\":")
                    .append(d.stream().partitionId()).append(",\"resumeAt\":").append(d.resumeAt())
                    .append('}');
        }
        out.append("],\"notBefore\":").append(notBefore).append(",\"fencedBy\":").append(fencedBy)
                .append(",\"closed\":").append(closed).append('}');
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void incarnation(StringBuilder out, Incarnation i) {
        out.append("{\"podId\":\"").append(i.podId()).append("\",\"podUid\":\"").append(i.podUid())
                .append("\",\"az\":\"").append(i.az()).append("\",\"endpoint\":\"")
                .append(i.endpoint()).append("\"}");
    }

    /**
     * ⚠️ REFUSES anything but the canonical form, as an {@link IOException}:
     * a field out of order, unknown or repeated, whitespace, an escape, and any
     * roster the record itself refuses.
     */
    public static Roster decode(byte[] bytes) throws IOException {
        JsonCursor c = JsonCursor.over(bytes, "roster");
        try {
            c.expect("{\"epoch\":");
            long epoch = c.readLong();
            c.expect(",\"predecessor\":");
            long predecessor = c.readLong();
            c.expect(",\"leader\":");
            Incarnation leader = readIncarnation(c, false).incarnation();
            c.expect(",\"members\":[");
            List<Member> members = new ArrayList<>();
            if (!c.take(']')) {
                do {
                    members.add(readIncarnation(c, true));
                } while (c.take(','));
                c.expect("]");
            }
            c.expect(",\"termRecord\":[");
            List<TermRecord> termRecord = new ArrayList<>();
            if (!c.take(']')) {
                do {
                    c.expect("{\"seq\":");
                    int seq = c.readInt();
                    c.expect(",\"walQuorum\":{");
                    SortedMap<UUID, Integer> q = TermRecord.byText();
                    if (!c.take('}')) {
                        do {
                            UUID index = readUuid(c);
                            c.expect(":");
                            if (q.put(index, c.readInt()) != null) {
                                throw c.refused("names an index twice in one record");
                            }
                        } while (c.take(','));
                        c.expect("}");
                    }
                    c.expect("}");
                    termRecord.add(new TermRecord(seq, q));
                } while (c.take(','));
                c.expect("]");
            }
            c.expect(",\"decisions\":[");
            List<Decision> decisions = new ArrayList<>();
            if (!c.take(']')) {
                do {
                    c.expect("{\"seq\":");
                    int seq = c.readInt();
                    c.expect(",\"index\":");
                    UUID index = readUuid(c);
                    c.expect(",\"partition\":");
                    int partition = c.readInt();
                    c.expect(",\"resumeAt\":");
                    long resumeAt = c.readLong();
                    c.expect("}");
                    decisions.add(new Decision(seq, new RunKey(index, partition), resumeAt));
                } while (c.take(','));
                c.expect("]");
            }
            c.expect(",\"notBefore\":");
            long notBefore = c.readLong();
            c.expect(",\"fencedBy\":");
            long fencedBy = c.readLong();
            c.expect(",\"closed\":");
            boolean closed = c.readBoolean();
            c.expect("}");
            c.end();
            Roster roster = new Roster(epoch, predecessor, leader, members, termRecord,
                    decisions, notBefore, fencedBy, closed);
            if (!java.util.Arrays.equals(roster.encode(), bytes)) {
                // ⚠️ THE BACKSTOP: anything the token checks above let through
                // that the encoder would not write -- a key order in a map, a
                // leading zero -- is refused rather than read.
                throw c.refused("is not in canonical form");
            }
            return roster;
        } catch (IllegalArgumentException invalid) {
            throw new IOException("not a valid roster: " + invalid.getMessage(), invalid);
        }
    }

    private static Member readIncarnation(JsonCursor c, boolean withState) throws IOException {
        c.expect("{\"podId\":");
        String podId = c.readString();
        c.expect(",\"podUid\":");
        String podUid = c.readString();
        c.expect(",\"az\":");
        String az = c.readString();
        c.expect(",\"endpoint\":");
        String endpoint = c.readString();
        State state = State.ROSTERED;
        if (withState) {
            c.expect(",\"state\":");
            String name = c.readString();
            if (name.equals("ROSTERED")) {
                state = State.ROSTERED;
            } else if (name.equals("DEPARTED")) {
                state = State.DEPARTED;
            } else {
                throw c.refused("has an unknown member state");
            }
        }
        c.expect("}");
        return new Member(new Incarnation(podId, podUid, az, endpoint), state);
    }

    private static UUID readUuid(JsonCursor c) throws IOException {
        String text = c.readString();
        UUID uuid;
        try {
            uuid = UUID.fromString(text);
        } catch (IllegalArgumentException e) {
            throw c.refused("has a malformed index UUID");
        }
        if (!uuid.toString().equals(text)) {
            throw c.refused("has a non-canonical index UUID");
        }
        return uuid;
    }

    /** {@code LATEST}'s content: {@code {"epoch":7}}. */
    public static byte[] encodeLatest(long epoch) {
        if (epoch < 1) {
            throw new IllegalArgumentException("LATEST names a term, at least 1: " + epoch);
        }
        return ("{\"epoch\":" + epoch + "}").getBytes(StandardCharsets.UTF_8);
    }

    public static long decodeLatest(byte[] bytes) throws IOException {
        JsonCursor c = JsonCursor.over(bytes, "LATEST");
        c.expect("{\"epoch\":");
        long epoch = c.readLong();
        c.expect("}");
        c.end();
        if (epoch < 1 || !java.util.Arrays.equals(encodeLatest(epoch), bytes)) {
            throw c.refused("is not a canonical term pointer");
        }
        return epoch;
    }
}
