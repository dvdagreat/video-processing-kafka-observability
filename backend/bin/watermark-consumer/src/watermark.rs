use anyhow::Result;

const FONT_FILE: &str = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf";

pub async fn apply_watermark(input: Vec<u8>) -> Result<Vec<u8>> {
    let drawtext = format!(
        "drawtext=fontfile={FONT_FILE}:text='WATERMARK':fontcolor=white:fontsize=24:\
         x=10:y=h-th-10:box=1:boxcolor=black@0.5:boxborderw=5"
    );
    common::ffmpeg::transcode_chunk(
        input,
        &[
            "-vf", &drawtext, "-c:v", "libx264", "-preset", "veryfast", "-c:a", "copy",
        ],
    )
    .await
}
