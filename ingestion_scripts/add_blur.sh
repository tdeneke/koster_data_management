#!/bin/bash

# Loop through files in directory
# and add blurred cropped areas with overlay
# onto original

for name in "$1"/*.mov; do
	ffmpeg -i "$name" -filter_complex \
	"[0:v]crop=iw:75:0:ih*(5/100),boxblur=luma_radius=5:chroma_radius=7:luma_power=1[b0]; \
	 [0:v]crop=iw:75:0:ih*(80/100),boxblur=luma_radius=5:chroma_radius=7:luma_power=1[b1]; \
	 [0:v][b0]overlay=0:H*(5/100)[ovr0]; \
	 [ovr0][b1]overlay=0:H*(80/100)[ovr1]" \
	-map "[ovr1]" -map 0:a -c:v libx264 -c:a copy -crf 30 -pix_fmt yuv420p -movflags +faststart "${name%.*}_blurred.mov"
	mv "${name}.mov" "${name%.*}_orig.mov" 
	mv "${name%.*}_blurred.mov" "${name}.mov"
done

