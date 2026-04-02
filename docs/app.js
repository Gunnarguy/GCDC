const state = {
  payload: null,
  activeView: "overview",
};

const elements = {};

document.addEventListener("DOMContentLoaded", () => {
  cacheElements();
  bindEvents();
  loadAtlas();
});

function cacheElements() {
  elements.statusBanner = document.querySelector("#statusBanner");
  elements.buildStamp = document.querySelector("#buildStamp");
  elements.sourceStamp = document.querySelector("#sourceStamp");
  elements.viewTabs = Array.from(document.querySelectorAll(".view-tab"));
  elements.viewPanels = Array.from(document.querySelectorAll(".view-panel"));

  elements.overviewCards = document.querySelector("#overviewCards");
  elements.topHeroesTable = document.querySelector("#topHeroesTable");
  elements.roleSummaryTable = document.querySelector("#roleSummaryTable");
  elements.variantMixList = document.querySelector("#variantMixList");
  elements.coverageLeadersTable = document.querySelector(
    "#coverageLeadersTable",
  );
  elements.patchCoverageTable = document.querySelector("#patchCoverageTable");
  elements.systemReferences = document.querySelector("#systemReferences");
  elements.releaseTimelineTable = document.querySelector(
    "#releaseTimelineTable",
  );
  elements.systemReferenceValuesTable = document.querySelector(
    "#systemReferenceValuesTable",
  );

  elements.searchHero = document.querySelector("#searchHero");
  elements.searchText = document.querySelector("#searchText");
  elements.searchSection = document.querySelector("#searchSection");
  elements.searchKind = document.querySelector("#searchKind");
  elements.searchLimit = document.querySelector("#searchLimit");
  elements.searchCards = document.querySelector("#searchCards");
  elements.searchPatches = document.querySelector("#searchPatches");
  elements.searchSections = document.querySelector("#searchSections");
  elements.searchSkills = document.querySelector("#searchSkills");
  elements.searchFeatures = document.querySelector("#searchFeatures");

  elements.heroSelect = document.querySelector("#heroSelect");
  elements.heroVariantSelect = document.querySelector("#heroVariantSelect");
  elements.heroCards = document.querySelector("#heroCards");
  elements.heroFeatures = document.querySelector("#heroFeatures");
  elements.heroSkills = document.querySelector("#heroSkills");
  elements.heroPatches = document.querySelector("#heroPatches");
  elements.heroSections = document.querySelector("#heroSections");

  elements.compareLeftHero = document.querySelector("#compareLeftHero");
  elements.compareRightHero = document.querySelector("#compareRightHero");
  elements.compareLeftHeading = document.querySelector("#compareLeftHeading");
  elements.compareRightHeading = document.querySelector("#compareRightHeading");
  elements.compareLeftProfile = document.querySelector("#compareLeftProfile");
  elements.compareRightProfile = document.querySelector("#compareRightProfile");
  elements.compareSummaryTable = document.querySelector("#compareSummaryTable");
  elements.compareLeftSkills = document.querySelector("#compareLeftSkills");
  elements.compareRightSkills = document.querySelector("#compareRightSkills");
}

function bindEvents() {
  elements.viewTabs.forEach((button) => {
    button.addEventListener("click", () => setView(button.dataset.view));
  });
  window.addEventListener("hashchange", syncViewFromHash);

  [
    elements.searchHero,
    elements.searchText,
    elements.searchSection,
    elements.searchKind,
    elements.searchLimit,
  ].forEach((control) => {
    control.addEventListener("input", () => renderSearch());
    control.addEventListener("change", () => renderSearch());
  });

  elements.heroSelect.addEventListener("change", () => {
    populateHeroVariantSelect();
    renderHero();
  });
  elements.heroVariantSelect.addEventListener("change", () => renderHero());
  elements.compareLeftHero.addEventListener("change", () => renderCompare());
  elements.compareRightHero.addEventListener("change", () => renderCompare());
}

async function loadAtlas() {
  setStatus("Loading exported atlas...", false);
  try {
    const response = await fetch("./data/atlas.json", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Unable to load atlas.json (${response.status})`);
    }
    state.payload = await response.json();
    hydrateControls();
    syncViewFromHash();
    renderOverview();
    renderSearch();
    renderHero();
    renderCompare();
    renderMeta();
    setStatus(
      "GitHub Pages atlas is ready. This static version keeps overview, search, and hero dossier access without a server.",
      false,
    );
  } catch (error) {
    console.error(error);
    setStatus(
      `Failed to load exported atlas: ${error.message}. Run \`make pages\` locally and republish GitHub Pages.`,
      true,
    );
  }
}

function renderMeta() {
  if (!state.payload) {
    return;
  }
  const generatedAt = new Date(state.payload.generated_at);
  elements.buildStamp.textContent = `Build: ${generatedAt.toLocaleString()}`;
  elements.sourceStamp.textContent = `Source: ${state.payload.source.database_path}`;
}

function hydrateControls() {
  if (!state.payload) {
    return;
  }

  const heroOptions = state.payload.heroes.map((hero) => hero.name_en);
  const heroOptionsHtml = heroOptions
    .map(
      (heroName) =>
        `<option value="${escapeAttribute(heroName)}">${escapeHtml(heroName)}</option>`,
    )
    .join("");

  elements.heroSelect.innerHTML = heroOptionsHtml;
  elements.heroSelect.value = heroOptions.includes("Ronan")
    ? "Ronan"
    : heroOptions[0] || "";

  elements.compareLeftHero.innerHTML = heroOptionsHtml;
  elements.compareRightHero.innerHTML = heroOptionsHtml;
  elements.compareLeftHero.value = heroOptions.includes("Ronan")
    ? "Ronan"
    : heroOptions[0] || "";
  elements.compareRightHero.value = heroOptions.includes("Amy")
    ? "Amy"
    : heroOptions.find(
        (heroName) => heroName !== elements.compareLeftHero.value,
      ) ||
      heroOptions[0] ||
      "";

  const kindOptions = [{ value: "", label: "All variants" }].concat(
    state.payload.variant_mix.map((row) => ({
      value: row.variant_kind,
      label: row.variant_kind_label,
    })),
  );
  elements.searchKind.innerHTML = kindOptions
    .map(
      (option) =>
        `<option value="${escapeAttribute(option.value)}">${escapeHtml(option.label)}</option>`,
    )
    .join("");

  populateHeroVariantSelect();
}

function populateHeroVariantSelect() {
  if (!state.payload) {
    return;
  }
  const heroName = elements.heroSelect.value;
  const heroSections = state.payload.sections.filter(
    (row) => row.name_en === heroName,
  );
  const variants = dedupeBy(heroSections, (row) => row.variant_title).map(
    (row) => ({
      title: row.variant_title,
      label: row.variant_label,
    }),
  );

  const options = [{ title: "", label: "All variants" }].concat(variants);
  const previousValue = elements.heroVariantSelect.value;
  elements.heroVariantSelect.innerHTML = options
    .map(
      (option) =>
        `<option value="${escapeAttribute(option.title)}">${escapeHtml(option.label)}</option>`,
    )
    .join("");

  if (options.some((option) => option.title === previousValue)) {
    elements.heroVariantSelect.value = previousValue;
  } else {
    elements.heroVariantSelect.value = "";
  }
}

function setView(viewName) {
  state.activeView = viewName;
  window.location.hash = viewName;
  applyViewState();
}

function syncViewFromHash() {
  const candidate = window.location.hash.replace("#", "");
  if (["overview", "search", "hero", "compare"].includes(candidate)) {
    state.activeView = candidate;
  }
  applyViewState();
}

function applyViewState() {
  elements.viewTabs.forEach((button) => {
    button.classList.toggle(
      "is-active",
      button.dataset.view === state.activeView,
    );
  });
  elements.viewPanels.forEach((panel) => {
    panel.classList.toggle(
      "is-hidden",
      panel.id !== `view-${state.activeView}`,
    );
  });
}

function renderOverview() {
  if (!state.payload) {
    return;
  }

  const summary = state.payload.summary;
  renderCards(elements.overviewCards, [
    {
      label: "Heroes",
      value: summary.hero_count,
      meta: "unified roster in exported atlas",
    },
    {
      label: "Variants",
      value: summary.variant_count,
      meta: "ranked base, former, and special units",
    },
    {
      label: "Sections",
      value: summary.section_count,
      meta: "captured heading blocks",
    },
    { label: "Skills", value: summary.skill_count, meta: "parsed skill rows" },
    {
      label: "Features",
      value: summary.feature_count,
      meta: "system availability markers",
    },
    {
      label: "System Refs",
      value: summary.system_reference_count,
      meta: "global growth and system notes",
    },
    {
      label: "Legacy Cells",
      value: summary.system_reference_value_count,
      meta: "flattened Hero Growth table values",
    },
    {
      label: "Release Rows",
      value: summary.release_history_count,
      meta: "parsed hero release chronology",
    },
    {
      label: "Patch Entries",
      value: summary.patch_entry_count,
      meta: `${formatNumber(summary.patch_block_count)} captured blocks with dated balance history`,
    },
  ]);

  renderTable(elements.topHeroesTable, state.payload.top_heroes, [
    ["meta_rank", "Rank"],
    ["variant_label", "Unit"],
    ["role", "Role"],
    ["rarity", "Rarity"],
    ["final_meta_score", "Meta Score"],
  ]);

  renderTable(elements.roleSummaryTable, state.payload.role_summary, [
    ["role", "Role"],
    ["unit_count", "Units"],
    ["avg_meta_score", "Avg Score"],
    ["best_meta_score", "Best Score"],
  ]);

  elements.variantMixList.innerHTML = state.payload.variant_mix
    .map(
      (row) => `
        <div class="stack-item">
          <strong>${escapeHtml(row.variant_kind_label)}</strong>
          <span>${formatNumber(row.variant_count)} pages</span>
        </div>
      `,
    )
    .join("");

  renderTable(elements.coverageLeadersTable, state.payload.section_coverage, [
    ["name_en", "Hero"],
    ["variants", "Variants"],
    ["section_blocks", "Sections"],
    ["patch_blocks", "Patch Blocks"],
    ["patch_entries", "Patch Entries"],
  ]);

  renderTable(
    elements.patchCoverageTable,
    state.payload.patch_coverage,
    [
      ["name_en", "Hero"],
      ["patch_entries", "Patch Entries"],
      ["patch_blocks", "Patch Blocks"],
      ["latest_patch_date", "Latest Patch"],
      ["latest_patch_type", "Latest Type"],
    ],
    "No parsed balance history was exported.",
  );

  renderReferenceEntries(
    elements.systemReferences,
    state.payload.system_references,
    "No global system reference rows were exported.",
  );

  renderTable(
    elements.releaseTimelineTable,
    state.payload.release_history.slice(0, 25),
    [
      ["release_year", "Year"],
      ["release_order_label", "Order"],
      ["hero_name_raw", "Hero"],
      ["release_date_text", "Date"],
    ],
    "No release timeline rows were exported.",
  );

  renderTable(
    elements.systemReferenceValuesTable,
    state.payload.system_reference_values.slice(0, 24),
    [
      ["title", "Table"],
      ["row_label", "Row"],
      ["column_label", "Column"],
      ["value_text", "Value"],
    ],
    "No structured legacy growth-table values were exported.",
  );
}

function renderSearch() {
  if (!state.payload) {
    return;
  }

  const heroQuery = normalize(elements.searchHero.value);
  const textQuery = normalize(elements.searchText.value);
  const sectionQuery = normalize(elements.searchSection.value);
  const kind = elements.searchKind.value;
  const limit = clampNumber(elements.searchLimit.value, 5, 100, 25);
  const highlightTerms = collectHighlightTerms(
    heroQuery,
    textQuery,
    sectionQuery,
  );

  let sections = state.payload.sections.slice();
  let skills = state.payload.skills.slice();
  let features = state.payload.features.slice();
  let patches = state.payload.patches.slice();

  if (heroQuery) {
    sections = sections.filter((row) => matchesHero(row, heroQuery));
    skills = skills.filter((row) => matchesHero(row, heroQuery));
    features = features.filter((row) => matchesHero(row, heroQuery));
    patches = patches.filter((row) => matchesHero(row, heroQuery));
  }

  if (kind) {
    sections = sections.filter((row) => row.variant_kind === kind);
    skills = skills.filter((row) => row.variant_kind === kind);
    features = features.filter((row) => row.variant_kind === kind);
    patches = patches.filter((row) => row.variant_kind === kind);
  }

  if (sectionQuery) {
    sections = sections.filter(
      (row) =>
        includesNormalized(row.section_path, sectionQuery) ||
        includesNormalized(row.heading_title, sectionQuery),
    );
    patches = patches.filter(
      (row) =>
        includesNormalized(row.section_path, sectionQuery) ||
        includesNormalized(row.heading_title, sectionQuery) ||
        includesNormalized(row.source_name, sectionQuery),
    );
  }

  if (textQuery) {
    sections = sections.filter(
      (row) =>
        includesNormalized(row.content, textQuery) ||
        includesNormalized(row.heading_title, textQuery),
    );
    skills = skills.filter(
      (row) =>
        includesNormalized(row.skill_name, textQuery) ||
        includesNormalized(row.description, textQuery),
    );
    features = features.filter(
      (row) =>
        includesNormalized(row.feature_key, textQuery) ||
        includesNormalized(row.feature_value, textQuery),
    );
    patches = patches.filter(
      (row) =>
        includesNormalized(row.patch_change, textQuery) ||
        includesNormalized(row.patch_change_type, textQuery) ||
        includesNormalized(row.source_name, textQuery) ||
        includesNormalized(row.body_excerpt, textQuery),
    );
  }

  renderCards(elements.searchCards, [
    {
      label: "Section Matches",
      value: sections.length,
      meta: "matching heading blocks",
    },
    { label: "Skill Matches", value: skills.length, meta: "parsed skill rows" },
    {
      label: "Feature Matches",
      value: features.length,
      meta: "availability markers",
    },
    {
      label: "Patch Matches",
      value: patches.length,
      meta: "dated balance changes",
    },
  ]);

  renderPatchTimeline(
    elements.searchPatches,
    patches.slice(0, limit),
    "No matching balance-history entries.",
    { highlightTerms },
  );

  renderSectionEntries(
    elements.searchSections,
    sections.slice(0, limit),
    "No matching section blocks.",
    {
      expandedByDefault: Boolean(textQuery || sectionQuery),
      highlightTerms,
    },
  );
  renderSkillCards(
    elements.searchSkills,
    skills.slice(0, limit),
    "No matching parsed skill rows.",
    { expandedByDefault: false, highlightTerms },
  );
  renderTable(
    elements.searchFeatures,
    features.slice(0, limit),
    [
      ["name_en", "Hero"],
      ["variant_label", "Variant"],
      ["feature_key", "Feature"],
      ["feature_value", "Value"],
    ],
    "No matching feature flags.",
    { highlightTerms },
  );
}

function renderHero() {
  if (!state.payload) {
    return;
  }

  const heroName = elements.heroSelect.value;
  const selectedVariantTitle = elements.heroVariantSelect.value;

  const heroSections = state.payload.sections.filter(
    (row) => row.name_en === heroName,
  );
  const heroVariants = state.payload.variants.filter(
    (row) => row.name_en === heroName,
  );
  const heroVariantScores = state.payload.variant_leaderboard.filter(
    (row) => row.name_en === heroName,
  );
  const heroSkills = state.payload.skills.filter(
    (row) => row.name_en === heroName,
  );
  const heroFeatures = state.payload.features.filter(
    (row) => row.name_en === heroName,
  );
  const heroPatchEntries = state.payload.patches.filter(
    (row) => row.name_en === heroName,
  );
  const heroRecord =
    state.payload.heroes.find((row) => row.name_en === heroName) || {};
  const selectedVariantRecord = selectedVariantTitle
    ? heroVariants.find((row) => row.variant_title === selectedVariantTitle) ||
      {}
    : heroVariants.find((row) => row.variant_kind === "base") ||
      heroVariants[0] ||
      {};
  const selectedVariantScoreRecord = selectedVariantTitle
    ? heroVariantScores.find(
        (row) => row.variant_title === selectedVariantTitle,
      ) || {}
    : heroVariantScores.find((row) => row.variant_kind === "base") ||
      heroVariantScores[0] ||
      {};

  const scopedSections = selectedVariantTitle
    ? heroSections.filter((row) => row.variant_title === selectedVariantTitle)
    : heroSections;
  const scopedSkills = selectedVariantTitle
    ? heroSkills.filter((row) => row.variant_title === selectedVariantTitle)
    : heroSkills;
  const scopedFeatures = selectedVariantTitle
    ? heroFeatures.filter((row) => row.variant_title === selectedVariantTitle)
    : heroFeatures;
  const scopedPatches = selectedVariantTitle
    ? heroPatchEntries.filter(
        (row) => row.variant_title === selectedVariantTitle,
      )
    : heroPatchEntries;

  const variantCount = dedupeBy(
    heroVariants,
    (row) => row.variant_title,
  ).length;
  const latestPatchDate = heroPatchEntries[0]?.patch_date || "-";
  const currentVariantLabel = selectedVariantTitle
    ? (
        heroSections.find(
          (row) => row.variant_title === selectedVariantTitle,
        ) || {}
      ).variant_label || "Selected variant"
    : "All variants";

  renderCards(elements.heroCards, [
    { label: "Hero", value: heroName, meta: currentVariantLabel },
    {
      label: "Role",
      value: selectedVariantRecord.role || heroRecord.role || "-",
      meta:
        selectedVariantRecord.rarity || heroRecord.rarity
          ? `Rarity ${selectedVariantRecord.rarity || heroRecord.rarity}`
          : "role metadata",
    },
    {
      label: "Meta Rank",
      value:
        selectedVariantScoreRecord.meta_rank || heroRecord.meta_rank || "-",
      meta: "lower is stronger",
    },
    {
      label: "Meta Score",
      value:
        selectedVariantScoreRecord.final_meta_score ||
        heroRecord.final_meta_score ||
        "-",
      meta: "exported leaderboard score",
    },
    {
      label: "Adventure",
      value:
        selectedVariantScoreRecord.adventure_tier ||
        heroRecord.adventure_tier ||
        "-",
      meta: "tier snapshot",
    },
    {
      label: "Battle",
      value:
        selectedVariantScoreRecord.battle_tier || heroRecord.battle_tier || "-",
      meta: "tier snapshot",
    },
    {
      label: "Boss",
      value:
        selectedVariantScoreRecord.boss_tier || heroRecord.boss_tier || "-",
      meta: "tier snapshot",
    },
    { label: "Variants", value: variantCount, meta: "ranked unit entries" },
    {
      label: "Sections",
      value: scopedSections.length,
      meta: "visible heading blocks",
    },
    { label: "Skills", value: scopedSkills.length, meta: "parsed skill rows" },
    { label: "Features", value: scopedFeatures.length, meta: "system markers" },
    {
      label: "Patch Entries",
      value: scopedPatches.length,
      meta:
        latestPatchDate === "-"
          ? "no parsed balance history"
          : `latest ${latestPatchDate}`,
    },
  ]);

  renderTable(
    elements.heroFeatures,
    scopedFeatures,
    [
      ["variant_label", "Variant"],
      ["feature_key", "Feature"],
      ["feature_value", "Value"],
    ],
    "No feature flags were exported for this hero scope.",
  );

  renderSkillCards(
    elements.heroSkills,
    scopedSkills,
    "No parsed skill rows were exported for this hero scope.",
    { expandedByDefault: false },
  );

  renderPatchTimeline(
    elements.heroPatches,
    scopedPatches,
    "No parsed balance history was exported for this hero scope.",
    { expandedByDefault: true },
  );

  renderSectionEntries(
    elements.heroSections,
    scopedSections,
    "No section blocks were exported for this hero scope.",
  );
}

function renderCompare() {
  if (!state.payload) {
    return;
  }

  const leftHeroName = elements.compareLeftHero.value;
  const rightHeroName = elements.compareRightHero.value;
  const leftBundle = buildHeroBundle(leftHeroName);
  const rightBundle = buildHeroBundle(rightHeroName);

  elements.compareLeftHeading.textContent = leftHeroName || "Left Hero";
  elements.compareRightHeading.textContent = rightHeroName || "Right Hero";

  renderHeroProfile(elements.compareLeftProfile, leftBundle);
  renderHeroProfile(elements.compareRightProfile, rightBundle);
  renderCompareSummary(elements.compareSummaryTable, leftBundle, rightBundle);
  renderSkillCards(
    elements.compareLeftSkills,
    leftBundle.skills.slice(0, 4),
    "No parsed skill rows were exported for this hero.",
    { expandedByDefault: false },
  );
  renderSkillCards(
    elements.compareRightSkills,
    rightBundle.skills.slice(0, 4),
    "No parsed skill rows were exported for this hero.",
    { expandedByDefault: false },
  );
}

function renderCards(container, cards) {
  container.innerHTML = cards
    .map(
      (card) => `
        <article class="stat-card">
          <p class="stat-label">${escapeHtml(String(card.label))}</p>
          <p class="stat-value">${escapeHtml(formatCardValue(card.value))}</p>
          <p class="stat-meta">${escapeHtml(String(card.meta || ""))}</p>
        </article>
      `,
    )
    .join("");
}

function renderTable(
  container,
  rows,
  columns,
  emptyText = "No rows available.",
  options = {},
) {
  if (!rows.length) {
    container.innerHTML = `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
    return;
  }

  const highlightTerms = options.highlightTerms || [];

  const header = columns
    .map(([, label]) => `<th>${escapeHtml(label)}</th>`)
    .join("");
  const body = rows
    .map(
      (row) => `
        <tr>
          ${columns
            .map(
              ([key]) =>
                `<td>${formatTableCell(row[key], highlightTerms)}</td>`,
            )
            .join("")}
        </tr>
      `,
    )
    .join("");

  container.innerHTML = `
    <div class="table-wrap">
      <table>
        <thead><tr>${header}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function renderSkillCards(container, rows, emptyText, options = {}) {
  if (!rows.length) {
    container.innerHTML = `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
    return;
  }

  const expandedByDefault = Boolean(options.expandedByDefault);
  const highlightTerms = options.highlightTerms || [];

  container.innerHTML = `
    <div class="skill-grid">
      ${rows
        .map((row, index) => {
          const fullDescription = formatReadableSkillText(
            row.description || "",
          );
          const previewText = formatReadableSkillPreview(fullDescription, 320);
          const normalizedSectionTitle = normalize(row.section_title || "");
          const normalizedSkillType = normalize(row.skill_type || "");
          const sectionTitle =
            row.section_title &&
            normalizedSectionTitle &&
            normalizedSectionTitle !== "skill" &&
            normalizedSectionTitle !== normalizedSkillType
              ? `<span class="pill">${escapeHtml(row.section_title)}</span>`
              : "";
          const openAttribute = expandedByDefault && index < 3 ? " open" : "";

          return `
            <details class="skill-card"${openAttribute}>
              <summary class="skill-summary">
                <div class="skill-topline">
                  <div class="skill-title-block">
                    <p class="skill-overline">${highlightText(formatHeroVariantHeading(row.name_en, row.variant_label), highlightTerms)}</p>
                    <h3>${highlightText(row.skill_name, highlightTerms)}</h3>
                  </div>
                  <span class="skill-kind">${escapeHtml(formatLabel(row.skill_type || "type unknown"))}</span>
                </div>
                <div class="skill-meta">
                  <span class="pill">${escapeHtml(formatLabel(row.skill_stage || "stage unknown"))}</span>
                  ${sectionTitle}
                </div>
                <p class="skill-description">${highlightText(previewText, highlightTerms)}</p>
              </summary>
              <div class="skill-body">
                <p class="skill-full-text">${highlightText(fullDescription, highlightTerms)}</p>
              </div>
            </details>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderPatchTimeline(container, rows, emptyText, options = {}) {
  if (!rows.length) {
    container.innerHTML = `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
    return;
  }

  const expandedByDefault = Boolean(options.expandedByDefault);
  const highlightTerms = options.highlightTerms || [];

  container.innerHTML = `
    <div class="patch-grid">
      ${rows
        .map((row, index) => {
          const openAttribute = expandedByDefault && index < 4 ? " open" : "";
          const patchDate = row.patch_date || "Undated";
          const patchType = row.patch_change_type || "Change";
          const sourceLabel =
            row.source_name || row.heading_title || "Captured block";
          const fullBodyText = formatReadableSkillText(row.body_text || "");

          return `
            <details class="patch-entry"${openAttribute}>
              <summary class="patch-summary">
                <div class="patch-topline">
                  <div class="patch-title-block">
                    <p class="patch-overline">${highlightText(formatHeroVariantHeading(row.name_en, row.variant_label), highlightTerms)}</p>
                    <h3>${highlightText(patchDate, highlightTerms)}</h3>
                  </div>
                  <span class="patch-type-chip patch-type-${patchTypeClassName(patchType)}">${escapeHtml(patchType)}</span>
                </div>
                <p class="patch-source">${highlightText(sourceLabel, highlightTerms)}</p>
              </summary>
              <div class="patch-body">
                <p class="patch-section-label">Patch Note</p>
                <p class="patch-change">${highlightText(formatReadablePatchText(row.patch_change || ""), highlightTerms)}</p>
                ${fullBodyText ? `<p class="patch-section-label">Captured Skill Text</p><p class="patch-body-text">${highlightText(fullBodyText, highlightTerms)}</p>` : ""}
                ${renderSourceLink(row.source_page)}
              </div>
            </details>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderSectionEntries(container, rows, emptyText, options = {}) {
  if (!rows.length) {
    container.innerHTML = `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
    return;
  }

  const highlightTerms = options.highlightTerms || [];
  const expandedByDefault = Boolean(options.expandedByDefault);

  container.innerHTML = `
    <div class="section-list">
      ${rows
        .map(
          (row, index) => `
            <details class="section-entry"${expandedByDefault && index < 2 ? " open" : ""}>
              <summary>
                ${highlightText(`${formatHeroVariantHeading(row.name_en, row.variant_label)} · ${row.heading_title}`, highlightTerms)}
              </summary>
              <div class="section-body">
                <p class="section-path">${highlightText(row.section_path, highlightTerms)}</p>
                <p class="section-preview">${highlightText(row.content_preview || "", highlightTerms)}</p>
                <p class="section-content">${highlightText(row.content || "", highlightTerms)}</p>
                ${renderSourceLink(row.source_page)}
              </div>
            </details>
          `,
        )
        .join("")}
    </div>
  `;
}

function renderReferenceEntries(container, rows, emptyText, options = {}) {
  if (!rows.length) {
    container.innerHTML = `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
    return;
  }

  const highlightTerms = options.highlightTerms || [];

  container.innerHTML = `
    <div class="section-list">
      ${rows
        .map(
          (row, index) => `
            <details class="section-entry"${index < 2 ? " open" : ""}>
              <summary>
                ${highlightText(`${row.title} · ${formatReferenceMeta(row)}`, highlightTerms)}
              </summary>
              <div class="section-body">
                <p class="section-path">${highlightText(row.section_path || "", highlightTerms)}</p>
                <p class="section-preview">${highlightText(row.content_preview || "", highlightTerms)}</p>
                <p class="section-content">${highlightText(row.content || "", highlightTerms)}</p>
                ${renderSourceLink(row.source_page)}
              </div>
            </details>
          `,
        )
        .join("")}
    </div>
  `;
}

function renderSourceLink(sourcePage) {
  if (!sourcePage) {
    return "";
  }
  return `<a class="section-link" href="${escapeAttribute(resolveSourceUrl(sourcePage))}" target="_blank" rel="noreferrer">Open source page</a>`;
}

function formatReadableSkillText(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .replace(/%(?=[A-Za-z])/g, "% ")
    .replace(/Patch Details\s*【Expand\/Collapse】/gi, "\n\nPatch Details\n")
    .replace(/\s+(\[[^\]]+\])/g, "\n$1")
    .replace(
      /\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2},\s+\d{4})\s+(Buff|Nerf|Others|Fix)\s*:/g,
      "\n$1 $2 $3:",
    )
    .replace(/\s{2,}/g, " ")
    .trim();
}

function formatReadablePatchText(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .replace(/\s+(Buff|Nerf|Others|Other|Fix|Hotfix)\s*:/gi, "\n$1:")
    .replace(/\s+-\s+/g, "\n- ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function formatReadableSkillPreview(value, maxLength) {
  const normalized = String(value || "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  if (normalized.length <= maxLength) {
    return normalized;
  }
  return `${normalized.slice(0, maxLength).trimEnd()}...`;
}

function formatLabel(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatFeatureKeyLabel(value) {
  return formatLabel(value).replaceAll("  ", " ");
}

function formatReferenceMeta(row) {
  const eraLabel = String(row.game_era || "")
    .replace(/_reference$/i, "")
    .replaceAll("_", " ")
    .trim();
  const parts = [formatLabel(eraLabel || "reference")];
  if (Number(row.is_legacy_system) === 1) {
    parts.push("Legacy");
  }
  if (row.trust_tier) {
    parts.push(formatLabel(String(row.trust_tier)));
  }
  return parts.join(" · ");
}

function formatHeroVariantHeading(heroName, variantLabel) {
  const hero = String(heroName || "").trim();
  const variant = String(variantLabel || "").trim();
  if (!hero) {
    return variant;
  }
  if (!variant) {
    return hero;
  }

  const normalizedHero = normalize(hero);
  const normalizedVariant = normalize(variant);
  if (
    normalizedVariant === normalizedHero ||
    normalizedVariant.startsWith(`${normalizedHero} ·`) ||
    normalizedVariant.startsWith(`${normalizedHero} -`)
  ) {
    return variant;
  }

  return `${hero} · ${variant}`;
}

function resolveSourceUrl(sourcePage) {
  const value = String(sourcePage || "").trim();
  if (!value) {
    return value;
  }
  if (/^https?:\/\//i.test(value)) {
    return value;
  }
  if (value.startsWith("/")) {
    return `https://en.namu.wiki${value}`;
  }
  return value;
}

function patchTypeClassName(value) {
  return normalize(value).replace(/[^a-z0-9]+/g, "-") || "change";
}

function collectHighlightTerms(...inputs) {
  const seen = new Set();
  const terms = [];

  inputs.forEach((input) => {
    const raw = String(input || "").trim();
    if (!raw) {
      return;
    }

    [raw, ...raw.split(/\s+/)]
      .map((term) => term.trim())
      .filter((term) => term.length >= 2)
      .forEach((term) => {
        const lowered = term.toLowerCase();
        if (seen.has(lowered)) {
          return;
        }
        seen.add(lowered);
        terms.push(term);
      });
  });

  return terms.sort((left, right) => right.length - left.length);
}

function highlightText(value, terms = []) {
  const text = String(value || "-");
  if (!terms.length || text === "-") {
    return escapeHtml(text);
  }

  const pattern = new RegExp(
    `(${terms.map((term) => escapeRegExp(term)).join("|")})`,
    "gi",
  );
  let cursor = 0;
  let html = "";

  for (const match of text.matchAll(pattern)) {
    const index = match.index ?? 0;
    html += escapeHtml(text.slice(cursor, index));
    html += `<mark class="match-mark">${escapeHtml(match[0])}</mark>`;
    cursor = index + match[0].length;
  }

  html += escapeHtml(text.slice(cursor));
  return html;
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildHeroBundle(heroName) {
  const hero =
    state.payload.heroes.find((row) => row.name_en === heroName) || {};
  const variants = state.payload.variants.filter(
    (row) => row.name_en === heroName,
  );
  const sections = state.payload.sections.filter(
    (row) => row.name_en === heroName,
  );
  const skills = state.payload.skills.filter((row) => row.name_en === heroName);
  const features = state.payload.features.filter(
    (row) => row.name_en === heroName,
  );
  const patches = state.payload.patches.filter(
    (row) => row.name_en === heroName,
  );
  const patchBlocks = dedupeBy(patches, (row) => row.patch_block_key);
  const latestPatchDate = patches[0]?.patch_date || "-";

  const variantLabels = dedupeBy(variants, (row) => row.variant_title).map(
    (row) => row.variant_label,
  );
  const featureLabels = dedupeBy(features, (row) => row.feature_key).map(
    (row) => formatFeatureKeyLabel(row.feature_key),
  );
  const skillMix = Array.from(
    countBy(skills, (row) => formatLabel(row.skill_type || "unknown")),
  ).map(([label, count]) => `${label} ${formatNumber(count)}`);
  const patchTypeMix = Array.from(
    countBy(patches, (row) => row.patch_change_type || "Change"),
  ).map(([label, count]) => `${label} ${formatNumber(count)}`);

  return {
    heroName,
    hero,
    variants,
    sections,
    skills,
    features,
    patches,
    patchBlocks,
    latestPatchDate,
    variantLabels,
    featureLabels,
    skillMix,
    patchTypeMix,
  };
}

function renderHeroProfile(container, bundle) {
  const variantMarkup = renderPillCluster(
    bundle.variantLabels,
    "No variants exported",
  );
  const featureMarkup = renderPillCluster(
    bundle.featureLabels,
    "No feature markers exported",
  );
  const skillMixMarkup = renderPillCluster(
    bundle.skillMix,
    "No parsed skill rows exported",
  );
  const patchMixMarkup = renderPillCluster(
    bundle.patchTypeMix,
    "No parsed balance history exported",
  );

  container.innerHTML = `
    <div class="profile-stack">
      <div>
        <p class="profile-overline">${escapeHtml(bundle.hero.role || "Unknown role")} · ${escapeHtml(bundle.hero.rarity || "Unknown rarity")}</p>
        <h3 class="profile-title">${escapeHtml(bundle.heroName)}</h3>
        <p class="profile-copy">${escapeHtml(bundle.hero.sources || "Static export from the local atlas database")}</p>
      </div>

      <div class="profile-metric-grid">
        ${renderProfileMetric("Meta Rank", bundle.hero.meta_rank || "-")}
        ${renderProfileMetric("Meta Score", formatCardValue(bundle.hero.final_meta_score || "-"))}
        ${renderProfileMetric("Variants", bundle.variantLabels.length)}
        ${renderProfileMetric("Sections", bundle.sections.length)}
        ${renderProfileMetric("Skills", bundle.skills.length)}
        ${renderProfileMetric("Features", bundle.features.length)}
        ${renderProfileMetric("Patch Entries", bundle.patches.length)}
        ${renderProfileMetric("Patch Blocks", bundle.patchBlocks.length)}
      </div>

      <div class="profile-pill-group">
        <p class="profile-group-label">Tier Snapshot</p>
        <div class="profile-pill-wrap">
          <span class="pill">Adventure ${escapeHtml(bundle.hero.adventure_tier || "-")}</span>
          <span class="pill">Battle ${escapeHtml(bundle.hero.battle_tier || "-")}</span>
          <span class="pill">Boss ${escapeHtml(bundle.hero.boss_tier || "-")}</span>
          <span class="pill">Latest Patch ${escapeHtml(bundle.latestPatchDate)}</span>
        </div>
      </div>

      <div class="profile-pill-group">
        <p class="profile-group-label">Variants</p>
        <div class="profile-pill-wrap">${variantMarkup}</div>
      </div>

      <div class="profile-pill-group">
        <p class="profile-group-label">Skill Mix</p>
        <div class="profile-pill-wrap">${skillMixMarkup}</div>
      </div>

      <div class="profile-pill-group">
        <p class="profile-group-label">System Markers</p>
        <div class="profile-pill-wrap">${featureMarkup}</div>
      </div>

      <div class="profile-pill-group">
        <p class="profile-group-label">Balance Mix</p>
        <div class="profile-pill-wrap">${patchMixMarkup}</div>
      </div>
    </div>
  `;
}

function renderCompareSummary(container, leftBundle, rightBundle) {
  const rows = [
    {
      label: "Role",
      left: leftBundle.hero.role || "-",
      right: rightBundle.hero.role || "-",
    },
    {
      label: "Meta Rank",
      left: leftBundle.hero.meta_rank || "-",
      right: rightBundle.hero.meta_rank || "-",
      winner: chooseWinner(
        leftBundle.hero.meta_rank,
        rightBundle.hero.meta_rank,
        "min",
      ),
    },
    {
      label: "Meta Score",
      left: leftBundle.hero.final_meta_score || "-",
      right: rightBundle.hero.final_meta_score || "-",
      winner: chooseWinner(
        leftBundle.hero.final_meta_score,
        rightBundle.hero.final_meta_score,
        "max",
      ),
    },
    {
      label: "Adventure Tier",
      left: leftBundle.hero.adventure_tier || "-",
      right: rightBundle.hero.adventure_tier || "-",
      winner: chooseWinner(
        tierWeight(leftBundle.hero.adventure_tier),
        tierWeight(rightBundle.hero.adventure_tier),
        "max",
      ),
    },
    {
      label: "Battle Tier",
      left: leftBundle.hero.battle_tier || "-",
      right: rightBundle.hero.battle_tier || "-",
      winner: chooseWinner(
        tierWeight(leftBundle.hero.battle_tier),
        tierWeight(rightBundle.hero.battle_tier),
        "max",
      ),
    },
    {
      label: "Boss Tier",
      left: leftBundle.hero.boss_tier || "-",
      right: rightBundle.hero.boss_tier || "-",
      winner: chooseWinner(
        tierWeight(leftBundle.hero.boss_tier),
        tierWeight(rightBundle.hero.boss_tier),
        "max",
      ),
    },
    {
      label: "Variants",
      left: leftBundle.variantLabels.length,
      right: rightBundle.variantLabels.length,
      winner: chooseWinner(
        leftBundle.variantLabels.length,
        rightBundle.variantLabels.length,
        "max",
      ),
    },
    {
      label: "Sections",
      left: leftBundle.sections.length,
      right: rightBundle.sections.length,
      winner: chooseWinner(
        leftBundle.sections.length,
        rightBundle.sections.length,
        "max",
      ),
    },
    {
      label: "Skills",
      left: leftBundle.skills.length,
      right: rightBundle.skills.length,
      winner: chooseWinner(
        leftBundle.skills.length,
        rightBundle.skills.length,
        "max",
      ),
    },
    {
      label: "Features",
      left: leftBundle.features.length,
      right: rightBundle.features.length,
      winner: chooseWinner(
        leftBundle.features.length,
        rightBundle.features.length,
        "max",
      ),
    },
    {
      label: "Patch Entries",
      left: leftBundle.patches.length,
      right: rightBundle.patches.length,
      winner: chooseWinner(
        leftBundle.patches.length,
        rightBundle.patches.length,
        "max",
      ),
    },
    {
      label: "Patch Blocks",
      left: leftBundle.patchBlocks.length,
      right: rightBundle.patchBlocks.length,
      winner: chooseWinner(
        leftBundle.patchBlocks.length,
        rightBundle.patchBlocks.length,
        "max",
      ),
    },
    {
      label: "Latest Patch",
      left: leftBundle.latestPatchDate,
      right: rightBundle.latestPatchDate,
    },
  ];

  container.innerHTML = `
    <div class="table-wrap">
      <table class="compare-table">
        <thead>
          <tr>
            <th>Metric</th>
            <th>${escapeHtml(leftBundle.heroName)}</th>
            <th>${escapeHtml(rightBundle.heroName)}</th>
          </tr>
        </thead>
        <tbody>
          ${rows
            .map(
              (row) => `
                <tr>
                  <td>${escapeHtml(row.label)}</td>
                  <td class="${row.winner === "left" ? "compare-winner" : ""}">${escapeHtml(formatCellValue(row.left))}</td>
                  <td class="${row.winner === "right" ? "compare-winner" : ""}">${escapeHtml(formatCellValue(row.right))}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderProfileMetric(label, value) {
  return `
    <div class="profile-metric">
      <p class="profile-metric-label">${escapeHtml(label)}</p>
      <p class="profile-metric-value">${escapeHtml(String(value))}</p>
    </div>
  `;
}

function renderPillCluster(items, emptyLabel) {
  if (!items.length) {
    return `<span class="pill">${escapeHtml(emptyLabel)}</span>`;
  }

  return items
    .map((item) => `<span class="pill">${escapeHtml(String(item))}</span>`)
    .join("");
}

function countBy(items, keyFn) {
  const counts = new Map();
  items.forEach((item) => {
    const key = keyFn(item);
    counts.set(key, (counts.get(key) || 0) + 1);
  });
  return counts;
}

function chooseWinner(leftValue, rightValue, mode) {
  if (
    leftValue === undefined ||
    leftValue === null ||
    rightValue === undefined ||
    rightValue === null ||
    leftValue === "-" ||
    rightValue === "-"
  ) {
    return null;
  }

  if (leftValue === rightValue) {
    return null;
  }

  if (mode === "min") {
    return leftValue < rightValue ? "left" : "right";
  }

  return leftValue > rightValue ? "left" : "right";
}

function tierWeight(value) {
  const weights = {
    ss: 6,
    s: 5,
    a: 4,
    b: 3,
    c: 2,
    d: 1,
  };
  return weights[normalize(value)] || 0;
}

function setStatus(message, isError) {
  elements.statusBanner.textContent = message;
  elements.statusBanner.classList.toggle("is-error", Boolean(isError));
}

function matchesHero(row, heroQuery) {
  return (
    includesNormalized(row.name_en, heroQuery) ||
    includesNormalized(row.name_ko, heroQuery)
  );
}

function includesNormalized(value, query) {
  return normalize(value).includes(query);
}

function normalize(value) {
  return String(value || "")
    .toLowerCase()
    .trim();
}

function formatCardValue(value) {
  if (typeof value === "number") {
    return formatNumber(value);
  }
  return String(value);
}

function formatCellValue(value) {
  if (typeof value === "number") {
    return Number.isInteger(value) ? formatNumber(value) : value.toFixed(2);
  }
  return String(value || "-");
}

function formatTableCell(value, highlightTerms) {
  if (typeof value === "number") {
    return escapeHtml(formatCellValue(value));
  }
  return highlightText(formatCellValue(value), highlightTerms);
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(value);
}

function clampNumber(value, min, max, fallback) {
  const numericValue = Number.parseInt(value, 10);
  if (Number.isNaN(numericValue)) {
    return fallback;
  }
  return Math.max(min, Math.min(max, numericValue));
}

function dedupeBy(items, keyFn) {
  const seen = new Set();
  const unique = [];
  for (const item of items) {
    const key = keyFn(item);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(item);
  }
  return unique;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value);
}
