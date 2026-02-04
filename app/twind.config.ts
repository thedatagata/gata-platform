// twind.config.ts
import { Options } from "$fresh/plugins/twind.ts";

export default {
  selfURL: import.meta.url,
  theme: {
    extend: {
      colors: {
        // Custom DATA_GATA color palette with semantic names
        "gata-dark": "#172217",      // Dark green
        "gata-green": "#90C137",     // Light green (primary)
        "gata-cream": "#F8F6F0",     // Off-white (background)
        "gata-hover": "#7dab2a",     // Darker green for hover states
        "gata-darker": "#0F3325",    // Darker green variant
        "gata-red": "#CD0402",       // Red accent
        "gata-charcoal": "#1C1D21",  // Charcoal
        "gata-lime": "#96ED89",      // Lime green
        "gata-yellow": "#BEDB39",    // Yellow-green
        "gata-chartreuse": "#A8C545", // Chartreuse
        "gata-sage": "#91C46C",      // Sage green
      },
      backgroundColor: {
        // Support arbitrary values used in components
        "[#172217]": "#172217",
        "[#90C137]": "#90C137",
        "[#F8F6F0]": "#F8F6F0",
        "[#7dab2a]": "#7dab2a",
        "[#a0d147]": "#a0d147",
        "[#186018]": "#186018",
        "[#172217]/95": "rgba(23, 34, 23, 0.95)",
        "[#90C137]/90": "rgba(144, 193, 55, 0.9)",
        "[#90C137]/20": "rgba(144, 193, 55, 0.2)",
        "[#90C137]/10": "rgba(144, 193, 55, 0.1)",
        "[#F8F6F0]/10": "rgba(248, 246, 240, 0.1)",
        "[#F8F6F0]/5": "rgba(248, 246, 240, 0.05)",
        // Semantic color names with opacity
        "gata-dark/95": "rgba(23, 34, 23, 0.95)",
        "gata-dark/80": "rgba(23, 34, 23, 0.8)",
        "gata-dark/60": "rgba(23, 34, 23, 0.6)",
        "gata-dark/40": "rgba(23, 34, 23, 0.4)",
        "gata-dark/50": "rgba(23, 34, 23, 0.5)",
        "gata-green/90": "rgba(144, 193, 55, 0.9)",
        "gata-green/20": "rgba(144, 193, 55, 0.2)",
        "gata-green/10": "rgba(144, 193, 55, 0.1)",
        "gata-red/20": "rgba(205, 4, 2, 0.2)",
        "gata-lime/20": "rgba(150, 237, 137, 0.2)",
        "gata-yellow/20": "rgba(190, 219, 57, 0.2)",
        "gata-chartreuse/20": "rgba(168, 197, 69, 0.2)",
        "gata-sage/20": "rgba(145, 196, 108, 0.2)",
      },
      textColor: {
        // Support arbitrary values used in components
        "[#172217]": "#172217",
        "[#90C137]": "#90C137",
        "[#F8F6F0]": "#F8F6F0",
        "[#F8F6F0]/90": "rgba(248, 246, 240, 0.9)",
        "[#F8F6F0]/80": "rgba(248, 246, 240, 0.8)",
        "[#F8F6F0]/70": "rgba(248, 246, 240, 0.7)",
        // Semantic color names with opacity
        "gata-cream/90": "rgba(248, 246, 240, 0.9)",
        "gata-cream/80": "rgba(248, 246, 240, 0.8)",
        "gata-cream/70": "rgba(248, 246, 240, 0.7)",
        "gata-cream/60": "rgba(248, 246, 240, 0.6)",
        "gata-cream/50": "rgba(248, 246, 240, 0.5)",
        "gata-cream/40": "rgba(248, 246, 240, 0.4)",
        // Data type colors
        "gata-lime": "#96ED89",
        "gata-sage": "#91C46C",
        "gata-chartreuse": "#A8C545",
        "gata-yellow": "#BEDB39",
      },
      borderColor: {
        // Support arbitrary values used in components
        "[#172217]": "#172217",
        "[#90C137]": "#90C137",
        "[#F8F6F0]": "#F8F6F0",
        "[#F8F6F0]/30": "rgba(248, 246, 240, 0.3)",
        "[#F8F6F0]/20": "rgba(248, 246, 240, 0.2)",
        "[#F8F6F0]/10": "rgba(248, 246, 240, 0.1)",
        "[#90C137]/50": "rgba(144, 193, 55, 0.5)",
        "[#90C137]/30": "rgba(144, 193, 55, 0.3)",
        // Semantic color names with opacity
        "gata-green/50": "rgba(144, 193, 55, 0.5)",
        "gata-green/30": "rgba(144, 193, 55, 0.3)",
        "gata-green/20": "rgba(144, 193, 55, 0.2)",
        "gata-green/10": "rgba(144, 193, 55, 0.1)",
        "gata-red/50": "rgba(205, 4, 2, 0.5)",
      },
    },
  },
} as Options;