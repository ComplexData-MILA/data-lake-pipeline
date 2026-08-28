/**
 * Chart palette + ink tokens (validated light/dark categorical palettes —
 * see dataviz reference: fixed order, never cycled; a 9th series folds into
 * "other"). Validated with scripts/validate_palette.js in both modes.
 */

export const CATEGORICAL_LIGHT = [
  "#2a78d6",
  "#eb6834",
  "#1baf7a",
  "#eda100",
  "#e87ba4",
  "#008300",
  "#4a3aa7",
  "#e34948",
];

export const CATEGORICAL_DARK = [
  "#3987e5",
  "#d95926",
  "#199e70",
  "#c98500",
  "#d55181",
  "#008300",
  "#9085e9",
  "#e66767",
];

export const OTHER_COLOR = "#898781";
export const SERIES_CAP = CATEGORICAL_LIGHT.length; // 8

export interface ChartInk {
  paper: string;
  plot: string;
  ink: string;
  secondary: string;
  muted: string;
  grid: string;
  axis: string;
  surface: string;
  palette: string[];
}

export function chartInk(dark: boolean): ChartInk {
  return {
    paper: "rgba(0,0,0,0)",
    plot: "rgba(0,0,0,0)",
    ink: dark ? "#ffffff" : "#0b0b0b",
    secondary: dark ? "#c3c2b7" : "#52514e",
    muted: "#898781",
    grid: dark ? "#2c2c2a" : "#e1e0d9",
    axis: dark ? "#383835" : "#c3c2b7",
    surface: dark ? "#1a1a19" : "#fcfcfb",
    palette: dark ? CATEGORICAL_DARK : CATEGORICAL_LIGHT,
  };
}
