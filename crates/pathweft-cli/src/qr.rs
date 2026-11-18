//! Terminal QR rendering for contact cards.

use anyhow::Result;
use qrcode::{Color, EcLevel, QrCode};

/// Module grid (true = dark), including the 4-module quiet zone scanners need.
pub fn modules(text: &str) -> Result<Vec<Vec<bool>>> {
    let code = QrCode::with_error_correction_level(text.as_bytes(), EcLevel::M)?;
    let width = code.width();
    let colors = code.to_colors();
    let quiet = 4;
    let size = width + 2 * quiet;
    Ok((0..size)
        .map(|y| {
            (0..size)
                .map(|x| {
                    x >= quiet
                        && y >= quiet
                        && x < quiet + width
                        && y < quiet + width
                        && colors[(y - quiet) * width + (x - quiet)] == Color::Dark
                })
                .collect()
        })
        .collect())
}

/// Two module rows per text line using half-block characters. Dark modules
/// are drawn as spaces on a light block, so it scans on dark terminals too.
pub fn render(text: &str) -> Result<String> {
    let grid = modules(text)?;
    let mut out = String::new();
    for pair in grid.chunks(2) {
        for x in 0..pair[0].len() {
            let top = pair[0][x];
            let bottom = pair.get(1).is_some_and(|row| row[x]);
            out.push(match (top, bottom) {
                (false, false) => '█',
                (true, false) => '▄',
                (false, true) => '▀',
                (true, true) => ' ',
            });
        }
        out.push('\n');
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_modules_decode_back_to_the_card() {
        let card = "pathweft:contact/1/0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20/89cc/alice";
        let grid = modules(card).unwrap();
        // Scale each module to 4x4 pixels so the decoder sees a clean image.
        let scale = 4;
        let size = grid.len() * scale;
        let mut image = rqrr::PreparedImage::prepare_from_greyscale(size, size, |x, y| {
            if grid[y / scale][x / scale] { 0 } else { 255 }
        });
        let grids = image.detect_grids();
        assert_eq!(grids.len(), 1);
        let (_, content) = grids[0].decode().unwrap();
        assert_eq!(content, card);
        assert!(render(card).unwrap().lines().count() * 2 >= grid.len());
    }
}
