const fs = require("fs");
const http = require("http");
const path = require("path");
const { spawn } = require("child_process");
const { chromium } = require("@playwright/test");

const projectRoot = path.resolve(__dirname, "..");
function findRepoRoot(startDir) {
  let current = startDir;
  while (true) {
    if (fs.existsSync(path.join(current, ".git"))) {
      return current;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      throw new Error(`Could not find repository root from ${startDir}`);
    }
    current = parent;
  }
}

const repoRoot = findRepoRoot(projectRoot);
const outputPath = path.resolve(
  process.env.PDF_OUTPUT || path.join(repoRoot, "docs", "Distributed_File_System_Presentation.pdf")
);
const baseUrl = process.env.PRESENTATION_URL || "http://127.0.0.1:4173/";
const outputDir = path.join(repoRoot, ".presentation-export");
const slideCount = Number(process.env.SLIDE_COUNT || "8");
const viewport = { width: 1440, height: 900 };

function waitForServer(url, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;

  return new Promise((resolve, reject) => {
    const tryOnce = () => {
      const req = http.get(url, (res) => {
        res.resume();
        if (res.statusCode && res.statusCode < 500) {
          resolve();
        } else {
          retry();
        }
      });

      req.on("error", retry);
      req.setTimeout(1000, () => {
        req.destroy();
        retry();
      });
    };

    const retry = () => {
      if (Date.now() > deadline) {
        reject(new Error(`Timed out waiting for ${url}`));
        return;
      }
      setTimeout(tryOnce, 250);
    };

    tryOnce();
  });
}

async function main() {
  fs.mkdirSync(outputDir, { recursive: true });
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });

  const server = spawn(
    process.platform === "win32" ? "npx.cmd" : "npx",
    ["vite", "preview", "--host", "127.0.0.1", "--port", "4173", "--strictPort"],
    {
      cwd: projectRoot,
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, BROWSER: "none" },
    }
  );

  let serverOutput = "";
  server.stdout.on("data", (chunk) => {
    serverOutput += chunk.toString();
  });
  server.stderr.on("data", (chunk) => {
    serverOutput += chunk.toString();
  });

  try {
    await waitForServer(baseUrl);

    const browser = await chromium.launch();
    const page = await browser.newPage({ viewport, deviceScaleFactor: 2 });
    await page.goto(baseUrl, { waitUntil: "networkidle" });

    const imagePaths = [];
    for (let i = 1; i <= slideCount; i += 1) {
      const imagePath = path.join(outputDir, `slide-${String(i).padStart(2, "0")}.png`);
      await page.screenshot({ path: imagePath, fullPage: true });
      imagePaths.push(imagePath);

      if (i < slideCount) {
        await page.keyboard.press("ArrowRight");
        await page.waitForTimeout(150);
      }
    }

    const htmlPath = path.join(outputDir, "slides.html");
    const imageSections = imagePaths
      .map((imagePath) => `<section class="slide"><img src="file://${imagePath}" /></section>`)
      .join("\n");

    fs.writeFileSync(
      htmlPath,
      `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    @page { size: 16in 10in; margin: 0; }
    html, body { margin: 0; padding: 0; background: #020617; }
    .slide { width: 16in; height: 10in; page-break-after: always; overflow: hidden; }
    .slide:last-child { page-break-after: auto; }
    img { width: 16in; height: 10in; object-fit: cover; display: block; }
  </style>
</head>
<body>
${imageSections}
</body>
</html>`
    );

    const pdfPage = await browser.newPage();
    await pdfPage.goto(`file://${htmlPath}`, { waitUntil: "networkidle" });
    await pdfPage.pdf({
      path: outputPath,
      printBackground: true,
      width: "16in",
      height: "10in",
      margin: { top: "0", right: "0", bottom: "0", left: "0" },
      preferCSSPageSize: true,
    });

    await browser.close();
    console.log(`Exported ${outputPath}`);
  } catch (error) {
    console.error(serverOutput);
    throw error;
  } finally {
    server.kill("SIGTERM");
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
