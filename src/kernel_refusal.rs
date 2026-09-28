//! The kernel's own count of datagrams its receive path refused for a socket.
//!
//! This crate accounts for two receive-side outcomes: the datagrams it read
//! ([`crate::ListenerStats::packets_received`]) and the ones its own per-flow
//! channels refused ([`crate::ListenerStats::packets_dropped_dispatcher_full`],
//! and per flow [`crate::ConnStats::packets_dropped_dispatcher_full`]). A
//! datagram the *kernel* refused — because the socket's receive queue was full
//! when it arrived — appears in neither: it never reached `recv`, so from
//! inside this crate it is indistinguishable from a datagram the path never
//! delivered.
//!
//! The kernel does count that refusal, per socket, and this module reads it
//! where the platform keeps one: the `drops` column of `/proc/net/udp` and
//! `/proc/net/udp6`, which is `sk_drops` — the same counter the `SO_RXQ_OVFL`
//! control message carries, but reachable with one file read per *sample*
//! instead of a control-message parse per *datagram*. A platform with no
//! per-socket count is reported as such; it is never approximated with a
//! host-wide total, which would be silently wrong for the listener that asked.

use crate::UnreliableTransmit;

/// Which kernel table a per-socket refusal count was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalSource {
    /// The `drops` column of `/proc/net/udp`, the kernel's IPv4 UDP socket
    /// table (`udp4_format_sock`, `sk_drops_read`).
    ProcNetUdp4,
    /// The same column of `/proc/net/udp6`, the IPv6 UDP socket table.
    ProcNetUdp6,
}

/// The kernel's own count of datagrams its receive path refused for one
/// listener's socket, before any `recv` could observe them.
///
/// With this reading and the peer's own count of what it sent, the receive side
/// reconciles three ways instead of two:
///
/// ```text
/// peer_offered     = packets_received + kernel_refused
/// packets_received = packets_dispatched + the drop counters in ListenerStats
/// ```
///
/// The residual after both subtractions is what the path lost. Before this
/// reading existed, `kernel_refused` and the path loss were one number, which
/// is why a receive buffer this process could not drain read as the path's
/// fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelRefused {
    /// A cumulative per-socket count from the kernel's own accounting. Monotone
    /// for the life of the socket, so the refusals in an interval are the
    /// difference of two readings.
    PerSocket {
        /// Datagrams the kernel refused for this socket since it was created.
        refused: u64,
        /// The kernel table the reading came from.
        source: RefusalSource,
    },
    /// This platform keeps no per-socket refusal count, so this crate reports
    /// none rather than substituting a host-wide total. `reason` names what the
    /// platform does keep, so the caller reads a named absence and not a zero.
    NotPerSocket {
        /// Why no per-socket reading exists on this platform.
        reason: &'static str,
    },
    /// A per-socket count exists on this platform but this listener's socket
    /// could not be named in it.
    Unidentified {
        /// What stopped the lookup.
        reason: &'static str,
    },
}

/// Why macOS reports no per-socket refusal count.
#[cfg(not(target_os = "linux"))]
const NOT_PER_SOCKET: &str = "macOS keeps no per-socket datagram-refusal count: `netstat -s -p udp` prints one \
     host-wide `dropped due to full socket buffers` total that every UDP socket on the host contributes \
     to, so it cannot be attributed to the listener that asks. This crate reports no number rather than \
     that total.";

/// The transport is not backed by a file descriptor (a test double), so there
/// is no socket to look up.
const NO_FD: &str = "the transport names no file descriptor, so its socket cannot be matched in the kernel's socket table";

/// The descriptor resolves to something that is not a socket entry.
const NOT_A_SOCKET: &str = "the socket's file descriptor does not resolve to a socket entry";

/// No row in either UDP socket table carries the socket's inode.
const ABSENT: &str = "the socket's inode is in neither /proc/net/udp nor /proc/net/udp6";

/// Two rows carry the socket's inode with different counts, which the kernel
/// does not produce: refused rather than resolved by picking one.
const CONFLICTING: &str = "two rows in the kernel's socket table carried this socket's inode with different refusal counts";

/// Whitespace-separated columns of a `/proc/net/udp{,6}` data row.
///
/// The header prints two of these columns split (`tx_queue rx_queue`,
/// `tr tm->when`) where a data row joins them with a colon, so a header-derived
/// index does not map onto a data row. A data row is indexed from its end
/// instead, and a row of any other width is refused rather than read at a
/// speculative offset — a kernel that appends a column must yield
/// [`KernelRefused::Unidentified`], not another column's number.
const FIELDS: usize = 13;

/// One row of a kernel UDP socket table, reduced to the two columns the reader
/// needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Row {
    pub(crate) inode: u64,
    pub(crate) drops: u64,
}

/// Decode one row of a `/proc/net/udp{,6}` table.
///
/// `None` for the header, a blank line, a row of the wrong width, or a row
/// whose trailing columns are not both integers: an unreadable row is not read
/// as zero.
pub(crate) fn parse_row(line: &str) -> Option<Row> {
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() != FIELDS {
        return None;
    }
    let mut tail = fields.iter().rev();
    let drops: u64 = tail.next()?.parse().ok()?;
    tail.next()?; // pointer
    tail.next()?; // ref
    let inode: u64 = tail.next()?.parse().ok()?;
    Some(Row { inode, drops })
}

/// The reading for one inode in a whole kernel socket table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lookup {
    /// Exactly one row named the inode; this is its refusal count.
    Found(u64),
    /// No row named the inode — the socket closed, or this is the other
    /// address family's table.
    Absent,
    /// Rows named the inode with different counts, which the kernel does not
    /// produce. Carries both so the caller can name them.
    Conflicting(u64, u64),
}

/// Find `inode` in a kernel UDP socket table and return its `drops` column.
pub(crate) fn lookup(table: &str, inode: u64) -> Lookup {
    let mut found: Option<u64> = None;
    for line in table.lines() {
        let Some(row) = parse_row(line) else {
            continue;
        };
        if row.inode != inode {
            continue;
        }
        match found {
            None => found = Some(row.drops),
            Some(previous) if previous == row.drops => {}
            Some(previous) => return Lookup::Conflicting(previous, row.drops),
        }
    }
    match found {
        Some(drops) => Lookup::Found(drops),
        None => Lookup::Absent,
    }
}

/// Decode the target of `/proc/self/fd/<n>` into the socket's inode.
///
/// A socket descriptor resolves to the literal `socket:[<inode>]`; anything
/// else — a file, a pipe, a failed read — is not a socket, and `None` says so
/// rather than guessing.
pub(crate) fn parse_socket_inode(link: &str) -> Option<u64> {
    link.strip_prefix("socket:[")?
        .strip_suffix(']')?
        .parse()
        .ok()
}

/// Read this listener's per-socket refusal count from the kernel's own tables.
#[cfg(target_os = "linux")]
pub(crate) fn read_socket<Utp: UnreliableTransmit + ?Sized>(utp: &Utp) -> KernelRefused {
    let Some(fd) = utp.raw_fd() else {
        return KernelRefused::Unidentified { reason: NO_FD };
    };
    let Ok(link) = std::fs::read_link(format!("/proc/self/fd/{fd}")) else {
        return KernelRefused::Unidentified {
            reason: NOT_A_SOCKET,
        };
    };
    let Some(inode) = parse_socket_inode(&link.to_string_lossy()) else {
        return KernelRefused::Unidentified {
            reason: NOT_A_SOCKET,
        };
    };
    // The inode is unique across both families, so searching both tables sidesteps
    // asking whether a dual-stack socket was filed under `udp` or `udp6`.
    for (path, source) in [
        ("/proc/net/udp", RefusalSource::ProcNetUdp4),
        ("/proc/net/udp6", RefusalSource::ProcNetUdp6),
    ] {
        let Ok(table) = std::fs::read_to_string(path) else {
            continue;
        };
        match lookup(&table, inode) {
            Lookup::Found(refused) => {
                return KernelRefused::PerSocket { refused, source };
            }
            Lookup::Absent => continue,
            Lookup::Conflicting(_, _) => {
                return KernelRefused::Unidentified {
                    reason: CONFLICTING,
                };
            }
        }
    }
    KernelRefused::Unidentified { reason: ABSENT }
}

/// No per-socket refusal count exists off Linux.
#[cfg(not(target_os = "linux"))]
pub(crate) fn read_socket<Utp: UnreliableTransmit + ?Sized>(_utp: &Utp) -> KernelRefused {
    KernelRefused::NotPerSocket {
        reason: NOT_PER_SOCKET,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real `/proc/net/udp` row, verbatim from a Linux 6.8 host
    /// (`head /proc/net/udp`; 13 whitespace-separated columns, `drops` last).
    const REAL_V4_ROW: &str = "    1: 00000000:FB49 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 9825184 2 ffff8c21484e2d00 4";
    /// A real IPv6 row's shape: the two addresses are 4 words each, so the row
    /// is still 13 columns with `drops` last (`__ip6_dgram_sock_seq_show`).
    const V6_ROW: &str = "    0: 00000000000000000000000000000000:1F90 00000000000000000000000000000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 4242 2 ffff8c21484e2d00 7";

    #[test]
    fn a_real_proc_row_decodes_to_its_inode_and_drops_column() {
        assert_eq!(
            parse_row(REAL_V4_ROW),
            Some(Row {
                inode: 9_825_184,
                drops: 4
            })
        );
        assert_eq!(
            parse_row(V6_ROW),
            Some(Row {
                inode: 4242,
                drops: 7
            })
        );
    }

    /// The header prints two columns split that a data row joins, so it is 15
    /// tokens and its trailing token is the literal `drops`; it must not decode
    /// as a row of zero drops.
    #[test]
    fn the_header_and_junk_are_refused_rather_than_read_as_zero() {
        let header = "   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops";
        assert_eq!(parse_row(header), None);
        assert_eq!(parse_row(""), None);
        assert_eq!(parse_row("nonsense"), None);
        // A row of the wrong width: a kernel that appended a column must not be
        // read at this parser's offsets.
        assert_eq!(parse_row(&format!("{REAL_V4_ROW} 5")), None);
        assert_eq!(
            parse_row(&REAL_V4_ROW.replace(" 9825184 ", " notanumber ")),
            None
        );
    }

    #[test]
    fn lookup_selects_by_inode_and_refuses_a_conflict() {
        let table = format!(
            "{}\n{REAL_V4_ROW}\n",
            "   sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode ref pointer drops"
        );
        assert_eq!(lookup(&table, 9_825_184), Lookup::Found(4));
        assert_eq!(lookup(&table, 123), Lookup::Absent);
        // Two rows for one inode with different counts cannot come from the
        // kernel; the resolution is to refuse, not to pick one.
        let twin = format!("{REAL_V4_ROW}\n{}", REAL_V4_ROW.replace(" 4", " 9"));
        assert_eq!(lookup(&twin, 9_825_184), Lookup::Conflicting(4, 9));
    }

    #[test]
    fn only_a_socket_entry_yields_an_inode() {
        assert_eq!(parse_socket_inode("socket:[9825184]"), Some(9_825_184));
        assert_eq!(parse_socket_inode("socket:[0]"), Some(0));
        assert_eq!(parse_socket_inode("pipe:[12345]"), None);
        assert_eq!(parse_socket_inode("socket:[abc]"), None);
        assert_eq!(parse_socket_inode("socket:[]"), None);
        assert_eq!(parse_socket_inode("/tmp/a.sock"), None);
    }
}
