//! Experimental Bluetooth LE link adapter for Pathweft.
//!
//! The adapter runs beside a node and carries its frames over one LE L2CAP
//! link. The node is unchanged: a route's next hop is the adapter's loopback
//! port. See `docs/ble-link.md`.

#[cfg(target_os = "linux")]
pub mod bluez;
pub mod bridge;
#[cfg(target_os = "macos")]
pub mod macos;
#[cfg(target_os = "macos")]
pub mod macos_peripheral;

/// Pathweft BLE test service; peers scan for this UUID.
pub const SERVICE_UUID: &str = "7e1f0001-5a7f-4c3b-9d1e-70a7f0e5b001";
/// GATT characteristic holding the L2CAP channel number (PSM), little-endian u16.
pub const PSM_CHARACTERISTIC_UUID: &str = "7e1f0002-5a7f-4c3b-9d1e-70a7f0e5b001";

/// Advertised local name carrying the channel number, so peers can connect
/// without first reading GATT (which BlueZ may attempt over classic Bluetooth
/// for dual-mode peers).
pub fn advertised_name(psm: u16) -> String {
    format!("pw-{psm}")
}

pub fn psm_from_name(name: &str) -> Option<u16> {
    name.strip_prefix("pw-")?
        .parse()
        .ok()
        .filter(|psm| *psm >= 0x80)
}

/// Android includes this two-byte little-endian value as service data in the
/// scan response. Reading it never requires a generic BlueZ Device.Connect.
pub fn psm_from_service_data(data: &[u8]) -> Option<u16> {
    let bytes: [u8; 2] = data.try_into().ok()?;
    let psm = u16::from_le_bytes(bytes);
    (0x80..=0xff).contains(&psm).then_some(psm)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn channel_service_data_rejects_wrong_size_and_reserved_values() {
        assert_eq!(psm_from_service_data(&[0x80, 0]), Some(128));
        assert_eq!(psm_from_service_data(&[0xff, 0]), Some(255));
        for invalid in [
            &[][..],
            &[0x80][..],
            &[0x80, 0, 0][..],
            &[0x7f, 0][..],
            &[0, 1][..],
        ] {
            assert_eq!(psm_from_service_data(invalid), None);
        }
    }
    #[test]
    fn channel_number_round_trips_through_the_advertised_name() {
        assert_eq!(psm_from_name(&advertised_name(192)), Some(192));
        assert_eq!(
            psm_from_name("pw-12"),
            None,
            "fixed/reserved PSMs are not dynamic"
        );
        assert_eq!(psm_from_name("ordinary-device"), None);
        assert!(
            advertised_name(u16::MAX).len() <= 8,
            "fits beside a 128-bit UUID in one advertisement"
        );
    }
}
