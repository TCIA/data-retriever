import { app } from '../../../wailsjs/go/models';

export function croissantSelectionSummary(items: app.CroissantDownloadOption[], selected: Set<string>): { count: number; size: string } {
  const included = items.filter(item => item.supported && selected.has(item.id));
  const known = included.filter(item => item.dataSizeKnown && Number.isFinite(item.dataSizeBytes) && item.dataSizeBytes >= 0);
  const unknown = known.length !== included.length;
  const bytes = known.reduce((sum, item) => sum + item.dataSizeBytes, 0);
  if (!included.length) return { count: 0, size: '0 B' };
  if (!known.length) return { count: included.length, size: 'Size unavailable' };
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const power = Math.min(units.length - 1, Math.max(0, Math.floor(Math.log10(bytes || 1) / 3)));
  const formatted = (bytes / Math.pow(1000, power)).toLocaleString(undefined, { maximumFractionDigits: 2 });
  return { count: included.length, size: `~${formatted} ${units[power]}${unknown ? ' + unknown size' : ''}` };
}
