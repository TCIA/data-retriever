import { Pipe, PipeTransform } from '@angular/core';

// Formats bytes to MB (< 1 GB) or GB (>= 1 GB) with one decimal place.
@Pipe({ name: 'bytes', pure: true })
export class BytesPipe implements PipeTransform {
  transform(value: number | null | undefined): string {
    const bytes = typeof value === 'number' && isFinite(value) && value >= 0 ? value : 0;
    const MB = 1024 * 1024;
    const GB = 1024 * 1024 * 1024;

    if (bytes < GB) {
      const mb = bytes / MB;
      return `${mb.toFixed(1)} MB`;
    }

    const gb = bytes / GB;
    return `${gb.toFixed(1)} GB`;
  }
}
