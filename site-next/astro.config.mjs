import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// Marketing pages live in src/pages; the documentation is Starlight under /docs/.
export default defineConfig({
  site: "https://ruviodb.com",
  output: "static",
  trailingSlash: "always",
  integrations: [
    starlight({
      title: "Ruvio Docs",
      logo: { src: "./public/images/ruvio-mark-128.png", alt: "Ruvio" },
      favicon: "/favicon.png",
      customCss: ["./src/styles/starlight.css"],
    }),
  ],
});
