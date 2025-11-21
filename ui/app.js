(() => {
  const ENTITY_COLORS = {
    ORGANIZATION: "#6BCB77",
    GEO: "#EE6C4D",
    PERSON: "#B084CC",
    DEFAULT: "#9AA0A6",
  };

  const state = {
    snapshot: null,
    cy: null,
    selectedEntity: null,
    selectedEdge: null,
  };

  const tooltipEl = document.getElementById("tooltip");
  const notificationEl = document.getElementById("notification");
  const panelEl = document.getElementById("panel-content");
  const searchForm = document.getElementById("search-form");
  const searchInput = document.getElementById("search-input");
  const exportPngBtn = document.getElementById("export-png");
  const downloadJsonBtn = document.getElementById("download-json");

  let notificationTimer = null;

  function escapeHTML(value) {
    if (value === null || value === undefined) {
      return "";
    }
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function showNotification(message, mode = "info", duration = 3200) {
    notificationEl.textContent = message;
    notificationEl.classList.remove("hidden", "error", "info");
    notificationEl.classList.add(mode);
    if (notificationTimer) {
      clearTimeout(notificationTimer);
    }
    notificationTimer = setTimeout(() => {
      notificationEl.classList.add("hidden");
    }, duration);
  }

  function hideTooltip() {
    tooltipEl.classList.add("hidden");
  }

  function positionTooltip(renderedPosition) {
    if (!state.cy || !renderedPosition) {
      return;
    }
    const rect = state.cy.container().getBoundingClientRect();
    const left = rect.left + window.scrollX + renderedPosition.x + 12;
    const top = rect.top + window.scrollY + renderedPosition.y + 12;
    tooltipEl.style.left = `${left}px`;
    tooltipEl.style.top = `${top}px`;
  }

  function showTooltip(text, renderedPosition) {
    tooltipEl.textContent = text;
    positionTooltip(renderedPosition);
    tooltipEl.classList.remove("hidden");
  }

  function formatStrength(strength) {
    if (strength === null || strength === undefined || Number.isNaN(strength)) {
      return "N/A";
    }
    return Number(strength).toFixed(2);
  }

  function computeEdgeWidth(strength) {
    if (strength === null || strength === undefined || Number.isNaN(strength)) {
      return 2;
    }
    const numeric = Number(strength);
    if (!Number.isFinite(numeric)) {
      return 2;
    }
    const clamped = Math.max(-1, Math.min(1, numeric));
    const magnitude = Math.abs(clamped);
    return 1 + magnitude * 3;
  }

  function buildElements(snapshot) {
    const nodeElements = snapshot.nodes.map((node) => ({
      data: {
        id: node.id,
        label: node.label,
        entity_type: node.entity_type || "UNKNOWN",
        claim_count:
          node.claim_count === null || node.claim_count === undefined
            ? null
            : node.claim_count,
      },
    }));

    const edgeElements = snapshot.edges.map((edge) => ({
      data: {
        id: edge.id,
        source: edge.source,
        target: edge.target,
        strength:
          edge.strength === null || edge.strength === undefined
            ? null
            : edge.strength,
        width: computeEdgeWidth(edge.strength),
      },
    }));

    return [...nodeElements, ...edgeElements];
  }

  function clearSelections() {
    if (!state.cy) {
      return;
    }
    state.cy.elements().removeClass("selected neighbor");
    state.cy.elements().unselect();
    state.selectedEntity = null;
    state.selectedEdge = null;
  }

  function highlightNode(node) {
    clearSelections();
    node.addClass("selected");
    const connectedEdges = node.connectedEdges();
    connectedEdges.addClass("selected");
    connectedEdges.connectedNodes().difference(node).addClass("neighbor");
    state.selectedEntity = node.id();
  }

  function highlightEdge(edge) {
    clearSelections();
    edge.addClass("selected");
    edge.connectedNodes().addClass("selected");
    state.selectedEdge = edge.id();
  }

  function focusNode(nodeId) {
    if (!state.cy) {
      return null;
    }
    const node = state.cy.getElementById(nodeId);
    if (!node || node.empty()) {
      showNotification(`Node "${nodeId}" is not present in the current snapshot.`, "error");
      return null;
    }
    highlightNode(node);
    const currentZoom = state.cy.zoom();
    const targetZoom = Math.max(currentZoom, 0.8);
    state.cy.animate(
      {
        center: { eles: node },
        zoom: targetZoom,
      },
      { duration: 280, easing: "ease-out" }
    );
    return node;
  }

  function renderDefaultPanel() {
    panelEl.innerHTML = `
      <h2>Knowledge Graph</h2>
      <p>Select a node or edge to inspect claims and related entities.</p>
    `;
  }

  function renderAliases(aliases) {
    if (!aliases || aliases.length === 0) {
      return `<p class="muted">No aliases recorded.</p>`;
    }
    const items = aliases
      .map((alias) => `<span class="alias-tag">${escapeHTML(alias)}</span>`)
      .join("");
    return `<div class="alias-tags">${items}</div>`;
  }

  function renderClaims(claims) {
    if (!claims || claims.length === 0) {
      return `<p class="muted">No claims recorded for this entity.</p>`;
    }
    return `
      <div class="claims-list">
        ${claims
          .map((claim) => {
            const source = claim.source ? escapeHTML(claim.source) : "Unknown source";
            const date = claim.claim_date ? escapeHTML(claim.claim_date) : "Unknown date";
            return `
              <article class="claim-item">
                <header>
                  <span>${date}</span>
                  <span class="claim-source">${source}</span>
                </header>
                <p>${escapeHTML(claim.content)}</p>
              </article>
            `;
          })
          .join("")}
      </div>
    `;
  }

  function renderRelated(relatedEntities) {
    if (!relatedEntities || relatedEntities.length === 0) {
      return `<p class="muted">No related entities detected.</p>`;
    }
    return `
      <ul class="related-list">
        ${relatedEntities
          .map(
            (entity) => `
              <li>
                <button
                  class="related-button"
                  type="button"
                  data-entity="${escapeHTML(entity)}"
                >
                  ${escapeHTML(entity)}
                </button>
              </li>
            `
          )
          .join("")}
      </ul>
    `;
  }

  function updateSidePanelWithEntity(entity) {
    panelEl.innerHTML = `
      <h2>${escapeHTML(entity.canonical)}</h2>
      <section class="panel-section">
        <h3>Aliases</h3>
        ${renderAliases(entity.aliases)}
      </section>
      <section class="panel-section">
        <h3>Claims</h3>
        ${renderClaims(entity.claims)}
      </section>
      <section class="panel-section">
        <h3>Related Entities</h3>
        ${renderRelated(entity.related_entities)}
      </section>
    `;
  }

  function updateSidePanelWithEdge(edge, strength) {
    panelEl.innerHTML = `
      <h2>${escapeHTML(edge.source)} — ${escapeHTML(edge.target)}</h2>
      <p class="muted">Relationship strength: ${formatStrength(strength)}</p>
      <section class="panel-section">
        <h3>Relationship Claims</h3>
        ${
          edge.claims && edge.claims.length > 0
            ? renderClaims(edge.claims)
            : `<p class="muted">No claims recorded for this relationship.</p>`
        }
      </section>
    `;
  }

  function initializeCytoscape(snapshot) {
    const elements = buildElements(snapshot);
    const cy = cytoscape({
      container: document.getElementById("graph"),
      elements,
      wheelSensitivity: 0.2,
      layout: {
        name: "cose",
        animate: false,
        randomize: true,
        padding: 80,
        edgeElasticity: 150,
        idealEdgeLength: 150,
        nodeRepulsion: 80000,
      },
      style: [
        {
          selector: "core",
          style: {
            "selection-box-color": "#7c3aed",
            "selection-box-border-color": "#93c5fd",
            "active-bg-color": "#93c5fd",
            "active-bg-opacity": 0.1,
          },
        },
        {
          selector: "node",
          style: {
            "width": 22,
            "height": 22,
            "background-color": ENTITY_COLORS.DEFAULT,
            "border-width": 1,
            "border-color": "#202634",
            "label": "data(label)",
            "color": "#ffffff",
            "font-size": 12,
            "font-weight": 600,
            "text-outline-width": 2,
            "text-outline-color": "rgba(15,20,29,0.9)",
            "text-valign": "bottom",
            "text-halign": "center",
            "text-margin-y": -10,
            "text-wrap": "wrap",
            "text-max-width": 110,
            "min-zoomed-font-size": 9,
          },
        },
        ...Object.entries(ENTITY_COLORS)
          .filter(([key]) => key !== "DEFAULT")
          .map(([type, color]) => ({
            selector: `node[entity_type = "${type}"]`,
            style: { "background-color": color },
          })),
        {
          selector: "node:selected",
          style: {
            "border-width": 4,
            "border-color": "#f4d35e",
            "shadow-blur": 18,
            "shadow-color": "#f4d35e",
            "shadow-opacity": 0.6,
          },
        },
        {
          selector: "node.neighbor",
          style: {
            "border-width": 3,
            "border-color": "#6bcb77",
          },
        },
        {
          selector: "edge",
          style: {
            "curve-style": "straight",
            "line-color": "#3b4252",
            "width": "data(width)",
            "opacity": 0.65,
          },
        },
        {
          selector: "edge.selected",
          style: {
            "line-color": "#f4d35e",
            "opacity": 0.95,
            "width": "mapData(width, 1, 5, 2.5, 6)",
          },
        },
      ],
    });

    cy.minZoom(0.08);
    cy.maxZoom(3);
    cy.userPanningEnabled(true);
    cy.userZoomingEnabled(true);

    cy.on("tap", "node", async (event) => {
      const nodeId = event.target.id();
      const entity = await fetchEntity(nodeId);
      if (!entity) {
        return;
      }
      const node = focusNode(entity.canonical);
      if (!node) {
        return;
      }
      updateSidePanelWithEntity(entity);
    });

    cy.on("tap", "edge", async (event) => {
      const edge = event.target;
      const src = edge.data("source");
      const tgt = edge.data("target");
      const details = await fetchEdge(src, tgt);
      if (!details) {
        return;
      }
      highlightEdge(edge);
      updateSidePanelWithEdge(details, edge.data("strength"));
    });

    cy.on("tap", (event) => {
      if (event.target === cy) {
        clearSelections();
        renderDefaultPanel();
      }
    });

    cy.on("mouseover", "node", (event) => {
      const node = event.target;
      const claimCount = node.data("claim_count");
      const label = node.data("label");
      const degree =
        state.snapshot?.adjacency?.[node.id()]?.length ?? node.connectedEdges().length;
      const entityType = node.data("entity_type");
      const tooltipText = `${label}\nType: ${entityType}\nClaims: ${
        claimCount ?? 0
      } • Degree: ${degree}`;
      showTooltip(tooltipText, event.renderedPosition);
    });

    cy.on("mouseout", "node", hideTooltip);

    cy.on("mouseover", "edge", (event) => {
      const edge = event.target;
      const tooltipText = `${edge.data("source")} — ${
        edge.data("target")
      }\nStrength: ${formatStrength(edge.data("strength"))}`;
      showTooltip(tooltipText, event.renderedPosition);
    });

    cy.on("mouseout", "edge", hideTooltip);

    cy.on("mousemove", "node", (event) => positionTooltip(event.renderedPosition));
    cy.on("mousemove", "edge", (event) => positionTooltip(event.renderedPosition));

    state.cy = cy;
    return cy;
  }

  async function fetchEntity(name) {
    try {
      const response = await fetch(`/api/entity/${encodeURIComponent(name)}`);
      if (!response.ok) {
        if (response.status === 404) {
          showNotification(`Entity "${name}" was not found.`, "error");
          return null;
        }
        throw new Error(`Request failed with status ${response.status}`);
      }
      return await response.json();
    } catch (error) {
      console.error(error);
      showNotification("Failed to load entity details.", "error");
      return null;
    }
  }

  async function fetchEdge(src, tgt) {
    try {
      const response = await fetch(
        `/api/edge?src=${encodeURIComponent(src)}&tgt=${encodeURIComponent(tgt)}`
      );
      if (!response.ok) {
        if (response.status === 404) {
          showNotification("Relationship not found.", "error");
          return null;
        }
        throw new Error(`Request failed with status ${response.status}`);
      }
      return await response.json();
    } catch (error) {
      console.error(error);
      showNotification("Failed to load relationship details.", "error");
      return null;
    }
  }

  async function loadSnapshot() {
    try {
      const response = await fetch("/api/graph/snapshot");
      if (!response.ok) {
        throw new Error(`Snapshot request failed: ${response.status}`);
      }
      const data = await response.json();
      state.snapshot = data;
      initializeCytoscape(data);
      renderDefaultPanel();
      showNotification("Graph snapshot loaded.", "info", 1800);
    } catch (error) {
      console.error(error);
      showNotification("Unable to load the graph snapshot.", "error", 5000);
    }
  }

  searchForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const term = searchInput.value.trim();
    if (!term) {
      return;
    }
    const entity = await fetchEntity(term);
    if (!entity) {
      return;
    }
    const node = focusNode(entity.canonical);
    if (!node) {
      return;
    }
    updateSidePanelWithEntity(entity);
    searchInput.value = entity.canonical;
  });

  panelEl.addEventListener("click", async (event) => {
    const button = event.target.closest(".related-button");
    if (!button) {
      return;
    }
    const entity = button.dataset.entity;
    if (!entity) {
      return;
    }
    const details = await fetchEntity(entity);
    if (!details) {
      return;
    }
    const node = focusNode(details.canonical);
    if (!node) {
      return;
    }
    updateSidePanelWithEntity(details);
  });

  exportPngBtn.addEventListener("click", () => {
    if (!state.cy) {
      showNotification("Graph is not ready yet.", "error");
      return;
    }
    const png = state.cy.png({ full: true, bg: "#0f141d", scale: 2 });
    const link = document.createElement("a");
    link.href = png;
    link.download = `graph-${Date.now()}.png`;
    link.click();
  });

  downloadJsonBtn.addEventListener("click", () => {
    if (!state.snapshot) {
      showNotification("Snapshot is not ready yet.", "error");
      return;
    }
    const blob = new Blob([JSON.stringify(state.snapshot, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `graph-snapshot-${Date.now()}.json`;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 0);
  });

  window.addEventListener("resize", () => {
    if (state.cy) {
      state.cy.resize();
    }
  });

  window.addEventListener("scroll", hideTooltip, true);

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      hideTooltip();
    }
  });

  window.addEventListener("DOMContentLoaded", loadSnapshot);
})();
