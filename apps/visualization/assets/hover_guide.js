(function () {
  const GRAPH_ID = "main-chart";
  const LINE_CLASS = "cross-subplot-hover-guide";
  const STYLE_ID = "cross-subplot-hover-guide-style";
  const DOCUMENT_BOUND_KEY = "crossSubplotHoverGuideDocumentBound";

  function ensureStyle() {
    if (document.getElementById(STYLE_ID)) {
      return;
    }
    const style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = `
      #${GRAPH_ID} .${LINE_CLASS} {
        position: absolute;
        width: 0;
        border-left: 1px dashed #666666;
        pointer-events: none;
        z-index: 20;
        display: none;
      }
    `;
    document.head.appendChild(style);
  }

  function graphDiv() {
    const root = document.getElementById(GRAPH_ID);
    return root && root.querySelector(".js-plotly-plot");
  }

  function ensureLine(gd) {
    if (getComputedStyle(gd).position === "static") {
      gd.style.position = "relative";
    }
    let line = gd.querySelector(`.${LINE_CLASS}`);
    if (!line) {
      line = document.createElement("div");
      line.className = LINE_CLASS;
      gd.appendChild(line);
    }
    return line;
  }

  function plotBounds(gd) {
    const size = gd._fullLayout && gd._fullLayout._size;
    if (!size) {
      return null;
    }
    return {
      top: size.t,
      bottom: gd.clientHeight - size.b,
    };
  }

  function showGuide(gd, clientX) {
    const bounds = plotBounds(gd);
    if (!bounds) {
      return;
    }
    const rect = gd.getBoundingClientRect();
    const x = clientX - rect.left;
    const line = ensureLine(gd);
    line.style.left = `${x}px`;
    line.style.top = `${bounds.top}px`;
    line.style.height = `${Math.max(0, bounds.bottom - bounds.top)}px`;
    line.style.display = "block";
  }

  function isInsidePlot(gd, event) {
    const bounds = plotBounds(gd);
    if (!bounds) {
      return false;
    }
    const rect = gd.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    return x >= 0 && x <= rect.width && y >= bounds.top && y <= bounds.bottom;
  }

  function hideGuide(gd) {
    const line = gd && gd.querySelector(`.${LINE_CLASS}`);
    if (line) {
      line.style.display = "none";
    }
  }

  function bind() {
    ensureStyle();
    const gd = graphDiv();
    if (!gd || gd.dataset.crossSubplotHoverGuideBound === "1") {
      return;
    }
    gd.dataset.crossSubplotHoverGuideBound = "1";

    gd.addEventListener("mousemove", function (event) {
      if (isInsidePlot(gd, event)) {
        showGuide(gd, event.clientX);
      }
    });
    gd.addEventListener("mouseleave", function () {
      hideGuide(gd);
    });
    gd.on("plotly_hover", function (eventData) {
      const sourceEvent = eventData && eventData.event;
      if (sourceEvent) {
        showGuide(gd, sourceEvent.clientX);
      }
    });

    if (document.documentElement.dataset[DOCUMENT_BOUND_KEY] !== "1") {
      document.documentElement.dataset[DOCUMENT_BOUND_KEY] = "1";
      document.addEventListener("mousemove", function (event) {
        const activeGraph = graphDiv();
        if (!activeGraph) {
          return;
        }
        if (isInsidePlot(activeGraph, event)) {
          showGuide(activeGraph, event.clientX);
        } else {
          hideGuide(activeGraph);
        }
      });
    }
  }

  const observer = new MutationObserver(bind);
  function start() {
    bind();
    observer.observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
