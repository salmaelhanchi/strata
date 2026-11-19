/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "media",
  theme: {
    extend: {
      colors: {
        ink: { DEFAULT: "#1d1d1b", soft: "#6b6a65" },
        paper: { DEFAULT: "#fbfaf7", card: "#ffffff", line: "#e4e1d8" },
        moss: { DEFAULT: "#2f6f5e", light: "#5fae96" },
        sand: { DEFAULT: "#c9b37e" },
        rust: { DEFAULT: "#a4452c" },
      },
      fontFamily: {
        mono: ["ui-monospace", "SFMono-Regular", "Consolas", "monospace"],
      },
    },
  },
};
