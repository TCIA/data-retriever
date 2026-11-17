import { app } from '../../../wailsjs/go/models';
import { croissantSelectionSummary } from './croissant-review';

describe('Croissant selection size', () => {
  const known = new app.CroissantDownloadOption({ id: 'known', supported: true, dataSizeKnown: true, dataSizeBytes: 1590000000 });
  const unknown = new app.CroissantDownloadOption({ id: 'unknown', supported: true, dataSizeKnown: false });
  const unsupported = new app.CroissantDownloadOption({ id: 'unsupported', supported: false, dataSizeKnown: true, dataSizeBytes: 445000000000 });

  it('does not hide unknown bytes inside an apparently complete total', () => {
    const result = croissantSelectionSummary([known, unknown, unsupported], new Set(['known', 'unknown', 'unsupported']));
    expect(result.count).toBe(2);
    expect(result.size).toContain('GB + unknown size');
  });

  it('excludes unchecked components and handles an empty selection', () => {
    expect(croissantSelectionSummary([known, unknown], new Set(['unknown'])).size).toBe('Size unavailable');
    expect(croissantSelectionSummary([known, unknown], new Set()).size).toBe('0 B');
  });

  it('rejects non-finite or negative supplied sizes', () => {
    for (const value of [NaN, Infinity, -1]) {
      const item = new app.CroissantDownloadOption({ id: 'bad', supported: true, dataSizeKnown: true, dataSizeBytes: value });
      expect(croissantSelectionSummary([item], new Set(['bad'])).size).toBe('Size unavailable');
    }
  });
});
