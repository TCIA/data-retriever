import { Pipe, PipeTransform } from '@angular/core';

@Pipe({ name: 'bytes', pure: true })
export class BytesPipe implements PipeTransform {
  transform(value: number | null | undefined, decimals = 2): string {
    const bytes = typeof value === 'number' && isFinite(value) && value >= 0 ? value : 0;
    const clampedDecimals = Math.max(0, Math.min(3, Math.floor(decimals)));
    const KB = 1024;
    const MB = 1024 * 1024;
    const GB = 1024 * 1024 * 1024;

    if (bytes < MB) {
      const kb = bytes / KB;
      return `${kb.toFixed(clampedDecimals)} KB`;
    }

    if (bytes < GB) {
      const mb = bytes / MB;
      return `${mb.toFixed(clampedDecimals)} MB`;
    }

    const gb = bytes / GB;
    return `${gb.toFixed(clampedDecimals)} GB`;
  }
}
