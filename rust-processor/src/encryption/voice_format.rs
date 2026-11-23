//! Inspect the bounded audio headers emitted by MediaRecorder. Playback and
//! packet decoding remain with the platform, outside the crypto worker.
use super::attachment_payload::MAX_MEDIA_BYTES;

#[cfg(test)]
pub(super) const WEBVIEW_SILENCE: &[u8] =
    include_bytes!("../../tests/fixtures/webview2-voice-silence.webm");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VoiceFormat {
    Ogg,
    WebM,
}

impl VoiceFormat {
    pub fn detect(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_MEDIA_BYTES {
            return None;
        }
        if ogg_opus(bytes) {
            Some(Self::Ogg)
        } else if webm_opus(bytes).is_some() {
            Some(Self::WebM)
        } else {
            None
        }
    }

    pub fn mime(self) -> &'static str {
        match self {
            Self::Ogg => "audio/ogg",
            Self::WebM => "audio/webm",
        }
    }

    pub fn filename(self) -> &'static str {
        match self {
            Self::Ogg => "voice.ogg",
            Self::WebM => "voice.webm",
        }
    }
}

fn opus_head(packet: &[u8]) -> bool {
    packet.len() == 19
        && packet.starts_with(b"OpusHead")
        && packet[8] == 1
        && (1..=2).contains(&packet[9])
        && packet[18] == 0
}

fn ogg_opus(bytes: &[u8]) -> bool {
    // The identification packet occupies its own beginning-of-stream page.
    bytes.starts_with(b"OggS\0\x02")
        && bytes.get(26..28) == Some(&[1, 19])
        && bytes.get(28..47).is_some_and(opus_head)
        && bytes
            .get(47..)
            .is_some_and(|rest| rest.len() >= 27 && rest.starts_with(b"OggS\0"))
}

fn vint(bytes: &mut &[u8], limit: usize, keep_marker: bool) -> Option<(u64, bool)> {
    let marker = 0x80u8.checked_shr(bytes.first()?.leading_zeros())?;
    let width = (marker.leading_zeros() + 1) as usize;
    if width > limit {
        return None;
    }
    let (encoded, rest) = bytes.split_at_checked(width)?;
    *bytes = rest;
    let first = if keep_marker {
        encoded[0]
    } else {
        encoded[0] & (marker - 1)
    };
    let value = encoded[1..].iter().fold(u64::from(first), |value, byte| {
        (value << 8) | u64::from(*byte)
    });
    Some((value, value == (1 << (width * 7)) - 1))
}

fn element<'a>(bytes: &mut &'a [u8]) -> Option<(u64, &'a [u8])> {
    let (id, _) = vint(bytes, 4, true)?;
    let (size, unknown) = vint(bytes, 8, false)?;
    if unknown {
        return None;
    }
    let (value, rest) = bytes.split_at_checked(usize::try_from(size).ok()?)?;
    *bytes = rest;
    Some((id, value))
}

fn child(mut bytes: &[u8], id: u64) -> Option<&[u8]> {
    while !bytes.is_empty() {
        let (tag, value) = element(&mut bytes)?;
        if tag == id {
            return Some(value);
        }
    }
    None
}

fn webm_opus(mut bytes: &[u8]) -> Option<()> {
    // MediaRecorder emits a short EBML header, a Segment (often unknown length),
    // and Tracks before the first Cluster. Avoid scanning media payloads.
    let (id, header) = element(&mut bytes)?;
    if id != 0x1a45dfa3 || header.len() > 4096 || child(header, 0x4282)? != b"webm" {
        return None;
    }
    let (segment, _) = vint(&mut bytes, 4, true)?;
    let (size, unknown) = vint(&mut bytes, 8, false)?;
    if segment != 0x18538067 {
        return None;
    }
    if !unknown {
        bytes = bytes.get(..usize::try_from(size).ok()?)?;
    }
    let mut prefix = &bytes[..bytes.len().min(64 * 1024)];
    while !prefix.is_empty() {
        let (id, tracks) = element(&mut prefix)?;
        if id == 0x1f43b675 {
            return None;
        }
        if id != 0x1654ae6b {
            continue;
        }
        let mut tracks = tracks;
        let (entry, track) = element(&mut tracks)?;
        return (entry == 0xae
            && tracks.is_empty()
            && child(track, 0x83)? == [2]
            && child(track, 0x86)? == b"A_OPUS"
            && opus_head(child(track, 0x63a2)?))
        .then_some(());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_webview2_opus_and_rejects_other_containers_and_track_codecs() {
        assert_eq!(
            VoiceFormat::detect(WEBVIEW_SILENCE),
            Some(VoiceFormat::WebM)
        );
        let mut invalid = WEBVIEW_SILENCE.to_vec();
        let codec = invalid.windows(6).position(|v| v == b"A_OPUS").unwrap();
        invalid[codec] = b'V';
        assert_eq!(VoiceFormat::detect(&invalid), None);
        assert_eq!(VoiceFormat::detect(b"\x1a\x45\xdf\xa3\xff"), None);
        assert_eq!(VoiceFormat::detect(b"OggS\x00\x02not a packet"), None);
    }

    #[test]
    fn ogg_identification_page_requires_opus_and_supported_channels() {
        let mut data = [0; 74];
        data[..6].copy_from_slice(b"OggS\0\x02");
        data[26..38].copy_from_slice(b"\x01\x13OpusHead\x01\x02");
        data[47..51].copy_from_slice(b"OggS");
        assert_eq!(VoiceFormat::detect(&data), Some(VoiceFormat::Ogg));
        data[37] = 0;
        assert_eq!(VoiceFormat::detect(&data), None);
    }
}
