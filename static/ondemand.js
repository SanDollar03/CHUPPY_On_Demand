(() => {
    function loadOnDemandConfig() {
        const defaults = {
            rootPath: "",
            maxDepth: 5,
            uploadAllowedDepth: 5,
            allowedExtensions: [],
            maxUploadFileSizeBytes: 100 * 1024 * 1024,
        };

        const el = document.getElementById("ondemand-config");
        if (!el) return defaults;

        try {
            return {
                ...defaults,
                ...(JSON.parse(el.textContent || "{}") || {}),
            };
        } catch (err) {
            console.error("OnDemand config parse error:", err);
            return defaults;
        }
    }

    const cfg = loadOnDemandConfig();
    const MAX_DEPTH = Number(cfg.maxDepth || 5);
    const UPLOAD_ALLOWED_DEPTH = Number(cfg.uploadAllowedDepth || 5);
    const ALLOWED_EXTENSIONS = Array.from(new Set((Array.isArray(cfg.allowedExtensions) ? cfg.allowedExtensions : [])
        .map((ext) => String(ext || "").trim().toLowerCase())
        .filter(Boolean)))
        .sort();
    const ALLOWED_EXTENSION_SET = new Set(ALLOWED_EXTENSIONS);
    const MAX_UPLOAD_FILE_SIZE_BYTES = Math.max(1, Number(cfg.maxUploadFileSizeBytes || (100 * 1024 * 1024)));
    const TREE_COLLAPSED_MARK = ">";
    const TREE_EXPANDED_MARK = "∨";
    const LV5_TREE_LABELS = {
        "安全": "💞 安全",
        "品質": "🏅 品質",
        "生産": "🏭 生産",
        "保全": "🪛 保全",
        "環境": "🌳 環境",
        "原価": "💰 原価",
        "人材育成": "🏫 人材育成",
        "安全健康": "💞 安全健康",
        "専門技能": "💪 専門技能",
        "TPS": "📊 TPS",
        "人事": "👤 人事",
        "総務": "🧑‍💻 総務",
    };

    const folderTree = document.getElementById("folderTree");
    const currentPath = document.getElementById("currentPath");
    const currentMeta = document.getElementById("currentMeta");

    const dropZone = document.getElementById("dropZone");
    const dropZoneSub = document.getElementById("dropZoneSub");
    const dropZoneExts = document.getElementById("dropZoneExts");
    const fileInput = document.getElementById("fileInput");
    const fileSelectBtn = document.getElementById("fileSelectBtn");
    const refreshBtn = document.getElementById("refreshBtn");

    const tabListBtn = document.getElementById("tabListBtn");
    const tabQueueBtn = document.getElementById("tabQueueBtn");
    const queueTabCount = document.getElementById("queueTabCount");
    const listPanel = document.getElementById("listPanel");
    const queuePanel = document.getElementById("queuePanel");

    const listEmpty = document.getElementById("listEmpty");
    const listTableWrap = document.getElementById("listTableWrap");
    const listState = document.getElementById("listState");
    const fileTableBody = document.getElementById("fileTableBody");
    const listSortButtons = Array.from(document.querySelectorAll('.tableSortBtn[data-sortable="1"]'));

    const queueSummary = document.getElementById("queueSummary");
    const queueHint = document.getElementById("queueHint");
    const queueEmpty = document.getElementById("queueEmpty");
    const queueTableWrap = document.getElementById("queueTableWrap");
    const queueTableBody = document.getElementById("queueTableBody");

    let selectedPath = "";
    let selectedDepth = 0;
    let selectedCanUpload = false;
    let queuePollTimer = null;
    let currentPanel = "list";
    let currentListDirs = [];
    let currentListFiles = [];
    let currentListSortKey = "mtime";
    let currentListSortDirection = "desc";
    const deleteBusyPaths = new Set();
    let folderStatusRefreshBusy = false;

    function syncOnDemandSidebarLayout() {
        const topbar = document.querySelector(".topbar");
        const h = topbar ? Math.ceil(topbar.getBoundingClientRect().height) : 0;
        document.documentElement.style.setProperty("--ondemand-topbar-h", `${h}px`);
    }

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/\"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function formatBytes(bytes) {
        const n = Number(bytes || 0);
        if (!n) return "-";
        const units = ["B", "KB", "MB", "GB", "TB"];
        let i = 0;
        let v = n;
        while (v >= 1024 && i < units.length - 1) {
            v /= 1024;
            i += 1;
        }
        return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
    }

    function setQueueHint(text, kind = "") {
        if (!queueHint) return;
        queueHint.textContent = text || "-";
        queueHint.className = `ondemandQueueHint${kind ? ` ${kind}` : ""}`;
    }

    function splitRelPath(relPath) {
        return String(relPath || "")
            .replace(/\\/g, "/")
            .replace(/^\/+|\/+$/g, "")
            .split("/")
            .filter(Boolean);
    }

    function formatRelativePath(relPath) {
        const rel = splitRelPath(relPath).join("/");
        return rel || ".";
    }

    function buildKnowledgeNameFromPath(relPath) {
        const parts = splitRelPath(relPath);
        if (parts.length !== UPLOAD_ALLOWED_DEPTH) return "";
        const filtered = parts.filter((part) => part !== "元データ");
        if (!filtered.length) return "";
        return `Chu_${filtered.join("_")}`;
    }

    async function fetchJson(url, options = {}) {
        const res = await fetch(url, {
            cache: "no-store",
            ...options,
        });
        let data = {};
        try {
            data = await res.json();
        } catch {
            data = {};
        }
        if (!res.ok || data.ok === false) {
            throw new Error(data.error || `${res.status} ${res.statusText}`);
        }
        return data;
    }

    async function fetchFolderData(path, options = {}) {
        const params = new URLSearchParams();
        params.set("path", path || "");
        if (options.forceStatus) {
            params.set("force_status", "1");
        }
        return fetchJson(`/api/explorer/list?${params.toString()}`);
    }

    function formatAllowedExtensions() {
        if (!ALLOWED_EXTENSIONS.length) return "-";
        return ALLOWED_EXTENSIONS.join(", ");
    }

    function formatUploadSizeLimit() {
        return formatBytes(MAX_UPLOAD_FILE_SIZE_BYTES);
    }

    function renderAllowedExtensions() {
        if (!dropZoneExts) return;
        dropZoneExts.textContent = `追加可能拡張子: ${formatAllowedExtensions()} / 1ファイル最大 ${formatUploadSizeLimit()}`;
    }

    async function ensureDatasetsLoaded() {
        return [];
    }

    async function updateKnowledgeLabel(_relPath, _canUpload) {
        return;
    }

    function setUploadState(canUpload, depth) {
        selectedCanUpload = !!canUpload;
        selectedDepth = Number(depth || 0);

        renderAllowedExtensions();

        if (selectedCanUpload) {
            dropZone.classList.remove("disabled");
            fileSelectBtn.disabled = false;
            dropZoneSub.textContent = `Lv${UPLOAD_ALLOWED_DEPTH}フォルダです。対応拡張子のみ追加できます。1ファイル ${formatUploadSizeLimit()} 以下のみ追加可能です。`;
        } else {
            dropZone.classList.add("disabled");
            fileSelectBtn.disabled = true;
            dropZoneSub.textContent = `Lv${UPLOAD_ALLOWED_DEPTH}フォルダ選択時のみ追加できます（現在: Lv${selectedDepth}）。1ファイル ${formatUploadSizeLimit()} を超えるものは追加できません。`;
        }
    }

    function setListVisible(visible) {
        if (visible) {
            listEmpty.classList.add("hidden");
            listTableWrap.classList.remove("hidden");
            listState.textContent = "表示中";
        } else {
            listTableWrap.classList.add("hidden");
            listEmpty.classList.remove("hidden");
            fileTableBody.innerHTML = "";
            listState.textContent = "未選択";
        }
    }

    function getFileExtension(name) {
        const base = String(name || "").trim().split(/[\\/]/).pop() || "";
        const index = base.lastIndexOf(".");
        if (index <= 0 || index === base.length - 1) return "";
        return base.slice(index + 1).toLowerCase();
    }

    function getDottedExtension(name) {
        const ext = getFileExtension(name);
        return ext ? `.${ext}` : "";
    }

    function isSupportedFileName(name) {
        const ext = getDottedExtension(name);
        return !!ext && ALLOWED_EXTENSION_SET.has(ext);
    }

    function getFileIcon(name, isDir = false) {
        if (isDir) return "📁";
        const ext = getFileExtension(name);
        if (["pdf"].includes(ext)) return "📕";
        if (["doc", "docx", "rtf"].includes(ext)) return "📘";
        if (["xls", "xlsx", "xlsm", "xlsb", "csv", "tsv"].includes(ext)) return "📗";
        if (["ppt", "pptx", "key"].includes(ext)) return "📙";
        if (["png", "jpg", "jpeg", "gif", "bmp", "svg", "webp", "tif", "tiff"].includes(ext)) return "🖼️";
        if (["zip", "rar", "7z", "gz", "tar"].includes(ext)) return "🗜️";
        if (["md", "txt", "log"].includes(ext)) return "📝";
        if (["json", "xml", "yaml", "yml", "ini", "conf"].includes(ext)) return "🧩";
        if (["py", "js", "ts", "css", "html", "java", "c", "cpp", "cs", "sql", "sh"].includes(ext)) return "💻";
        return "📄";
    }

    function renderNameCell(name, isDir = false) {
        const safeName = escapeHtml(name || "-");
        const icon = escapeHtml(getFileIcon(name, isDir));
        return `
            <div class="fileNameCell">
                <span class="fileTypeIcon" aria-hidden="true">${icon}</span>
                <span class="fileNameText">${safeName}</span>
            </div>
        `;
    }

    function normalizeRegistrationStatusCode(item) {
        const code = String(item?.registration_status || "").toLowerCase();
        if (["registered", "unregistered", "error", "registering"].includes(code)) return code;
        return "unregistered";
    }

    function registrationStatusLabel(item) {
        const code = normalizeRegistrationStatusCode(item);
        const label = String(item?.registration_status_label || "").trim();
        if (label) return label;
        if (code === "registered") return "変換済";
        if (code === "registering") return "変換中";
        if (code === "error") return "エラー";
        return "未変換";
    }

    function registrationStatusClass(item) {
        const code = normalizeRegistrationStatusCode(item);
        if (code === "registered") return "ok";
        if (code === "registering") return "info";
        if (code === "error") return "err";
        return "skip";
    }

    function registrationStatusTitle(item) {
        const parts = [];
        const detail = String(item?.registration_status_detail || "").trim();
        const stage = String(item?.registration_queue_stage || "").trim();
        const queueStatus = String(item?.registration_queue_status || "").trim();
        const queueOrder = Number(item?.registration_queue_order || 0);
        if (detail) parts.push(detail);
        if (stage) parts.push(`段階: ${stage}`);
        if (queueStatus) parts.push(`キュー状態: ${queueStatus}`);
        if (queueOrder > 0) parts.push(`順番: #${queueOrder}`);
        return parts.join(" / ") || registrationStatusLabel(item);
    }

    function renderRegistrationStatusCell(item, isDir = false) {
        if (isDir) {
            return `<span class="queueStatus skip">-</span>`;
        }
        const klass = escapeHtml(registrationStatusClass(item));
        const label = escapeHtml(registrationStatusLabel(item));
        const title = escapeHtml(registrationStatusTitle(item));
        return `<span class="queueStatus ${klass}" title="${title}">${label}</span>`;
    }

    function getRegistrationStatusSortRank(item, isDir = false) {
        if (isDir) return -1;
        const code = normalizeRegistrationStatusCode(item);
        if (code === "registered") return 1;
        if (code === "registering") return 2;
        if (code === "error") return 3;
        return 4;
    }

    function getListItemSortValue(item, sortKey) {
        const isDir = String(item?._rowType || "") === "dir";
        if (sortKey === "name") return String(item?.name || "").toLocaleLowerCase("ja");
        if (sortKey === "status") return getRegistrationStatusSortRank(item, isDir);
        if (sortKey === "size") return Number(isDir ? (item?.total_size_bytes || 0) : (item?.size_bytes || 0));
        const raw = String(item?.mtime || "").replace(" ", "T");
        const time = Date.parse(raw);
        return Number.isFinite(time) ? time : 0;
    }

    function compareListItems(a, b) {
        const av = getListItemSortValue(a, currentListSortKey);
        const bv = getListItemSortValue(b, currentListSortKey);
        let cmp = 0;
        if (typeof av === "number" && typeof bv === "number") {
            cmp = av === bv ? 0 : (av < bv ? -1 : 1);
        } else {
            cmp = String(av).localeCompare(String(bv), "ja", { numeric: true, sensitivity: "base" });
        }
        if (!cmp) {
            const byName = String(a?.name || "").localeCompare(String(b?.name || ""), "ja", { numeric: true, sensitivity: "base" });
            if (byName) cmp = byName;
        }
        if (!cmp) {
            const at = String(a?._rowType || "");
            const bt = String(b?._rowType || "");
            cmp = at.localeCompare(bt);
        }
        return currentListSortDirection === "desc" ? -cmp : cmp;
    }

    function applyListSortIndicators() {
        for (const btn of listSortButtons) {
            const sortKey = String(btn?.dataset?.sortKey || "");
            const active = sortKey === currentListSortKey;
            btn.classList.toggle("is-active", active);
            btn.dataset.sortDir = active ? currentListSortDirection : "";
            const mark = btn.querySelector(".tableSortMark");
            if (mark) {
                mark.textContent = active ? (currentListSortDirection === "asc" ? "▲" : "▼") : "⇅";
            }
        }
    }

    function setListSort(sortKey) {
        const nextKey = String(sortKey || "").trim();
        if (!nextKey) return;
        if (currentListSortKey === nextKey) {
            currentListSortDirection = currentListSortDirection === "asc" ? "desc" : "asc";
        } else {
            currentListSortKey = nextKey;
            currentListSortDirection = nextKey === "mtime" ? "desc" : "asc";
        }
        applyListSortIndicators();
        if (selectedCanUpload) {
            renderTable(currentListDirs, currentListFiles);
        }
    }

    function buildListStateText(dirs, files) {
        const counts = {
            registered: 0,
            registering: 0,
            error: 0,
            unregistered: 0,
        };
        for (const file of files) {
            const code = normalizeRegistrationStatusCode(file);
            counts[code] = Number(counts[code] || 0) + 1;
        }
        return [
            `フォルダ=${dirs.length}`,
            `ファイル=${files.length}`,
            `変換済=${counts.registered}`,
            `変換中=${counts.registering}`,
            `エラー=${counts.error}`,
            `未変換=${counts.unregistered}`,
        ].join(" / ");
    }

    function renderTable(dirs, files) {
        currentListDirs = Array.isArray(dirs) ? dirs.slice() : [];
        currentListFiles = Array.isArray(files) ? files.slice() : [];
        fileTableBody.innerHTML = "";

        const visibleDirs = currentListDirs.slice();
        const visibleFiles = currentListFiles.filter((item) => isSupportedFileName(item?.name || ""));
        const rows = [
            ...visibleDirs.map((item) => ({ ...item, _rowType: "dir" })),
            ...visibleFiles.map((item) => ({ ...item, _rowType: "file" })),
        ].sort(compareListItems);

        listState.textContent = buildListStateText(visibleDirs, visibleFiles);

        if (!rows.length) {
            const tr = document.createElement("tr");
            tr.innerHTML = `<td colspan="5" class="empty-cell">対応ファイルはありません。</td>`;
            fileTableBody.appendChild(tr);
            return;
        }

        for (const item of rows) {
            if (String(item?._rowType || "") === "dir") {
                const tr = document.createElement("tr");
                tr.className = "clickable-row";
                tr.innerHTML = `
                    <td>${renderNameCell(item.name, true)}</td>
                    <td>${renderRegistrationStatusCell(null, true)}</td>
                    <td>${escapeHtml(item.mtime || "-")}</td>
                    <td>-</td>
                    <td>-</td>
                `;
                tr.addEventListener("click", () => loadFolder(item.path || ""));
                fileTableBody.appendChild(tr);
                continue;
            }

            const filePath = String(item?.path || "");
            const isBusy = deleteBusyPaths.has(filePath);
            const disabled = isBusy;
            const disabledReason = isBusy ? "削除中です。" : "";
            const title = disabled ? disabledReason : "元ファイルとMarkdownを削除します。";
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${renderNameCell(item.name, false)}</td>
                <td>${renderRegistrationStatusCell(item, false)}</td>
                <td>${escapeHtml(item.mtime || "-")}</td>
                <td>${escapeHtml(formatBytes(item.size_bytes))}</td>
                <td>
                    <button
                        type="button"
                        class="fileDeleteBtn"
                        data-path="${escapeHtml(filePath)}"
                        title="${escapeHtml(title)}"
                        ${disabled ? "disabled" : ""}
                    >削除</button>
                </td>
            `;
            const btn = tr.querySelector(".fileDeleteBtn");
            if (btn) {
                btn.addEventListener("click", async (e) => {
                    e.stopPropagation();
                    await deleteFile(item);
                });
            }
            fileTableBody.appendChild(tr);
        }
    }

    function matchesLevelRule(item) {
        const depth = Number(item?.depth || 0);
        const name = String(item?.name || "");
        if (depth === 1) return [...name].length === 1;
        if (depth === 2) return [...name].length === 2;
        if (depth === 4) return name === "元データ";
        return true;
    }

    function filterDirsByRule(dirs) {
        return (dirs || []).filter(matchesLevelRule);
    }

    function closeSiblingBranches(currentWrapper) {
        const parentChildren = currentWrapper.parentElement;
        if (!parentChildren) return;
        const siblings = parentChildren.querySelectorAll(":scope > .folderTreeItem.expanded");
        siblings.forEach((sib) => {
            if (sib === currentWrapper) return;
            setTreeToggleState(sib, false);
        });
    }

    function closeAllOtherBranches(currentWrapper) {
        const expanded = folderTree.querySelectorAll(".folderTreeItem.expanded");
        expanded.forEach((item) => {
            if (item === currentWrapper) return;
            if (item.contains(currentWrapper)) return;
            if (currentWrapper.contains(item)) return;
            setTreeToggleState(item, false);
        });
    }

    function renderTreeLabel(item) {
        const rawName = String(item?.name || "/");
        const depth = Number(item?.depth || 0);
        const displayName = depth === UPLOAD_ALLOWED_DEPTH && Object.prototype.hasOwnProperty.call(LV5_TREE_LABELS, rawName)
            ? LV5_TREE_LABELS[rawName]
            : rawName;
        const name = escapeHtml(displayName);
        const count = Number(item?.file_count || 0);
        return `
            <span class="folderTreeMain">
                <span class="folderTreeName">${name}</span>
                <span class="folderTreeCount">(${escapeHtml(count)})</span>
            </span>
            <span class="folderTreeLv">Lv${escapeHtml(depth)}</span>
        `;
    }

    function setTreeToggleState(wrapper, expanded) {
        if (!wrapper) return;
        const toggle = wrapper.querySelector(":scope > .folderTreeRow > .folderTreeToggle");
        const nodeBtn = wrapper.querySelector(":scope > .folderTreeRow > .folderTreeNode");
        const children = wrapper.querySelector(":scope > .folderTreeChildren");
        const canExpand = !!(toggle && !toggle.disabled && children);

        if (!canExpand) {
            if (toggle) toggle.textContent = "";
            if (nodeBtn) {
                nodeBtn.setAttribute("aria-expanded", "false");
                nodeBtn.removeAttribute("data-expanded");
            }
            if (children) children.hidden = true;
            wrapper.classList.remove("expanded");
            return;
        }

        wrapper.classList.toggle("expanded", !!expanded);
        children.hidden = !expanded;
        toggle.textContent = expanded ? TREE_EXPANDED_MARK : TREE_COLLAPSED_MARK;
        if (nodeBtn) {
            nodeBtn.setAttribute("aria-expanded", expanded ? "true" : "false");
            nodeBtn.dataset.expanded = expanded ? "1" : "0";
        }
    }

    async function ensureTreeChildrenLoaded(wrapper, item) {
        const children = wrapper?.querySelector(":scope > .folderTreeChildren");
        if (!children || children.dataset.loaded) return;
        const childItems = await fetchTreeChildren(item);
        children.innerHTML = "";
        for (const child of childItems) {
            if (Number(child?.depth || 0) <= MAX_DEPTH) {
                children.appendChild(makeTreeNode(child));
            }
        }
        children.dataset.loaded = "1";
    }

    async function setTreeExpanded(wrapper, item, expand, options = {}) {
        const keepSiblingsOpen = !!options.keepSiblingsOpen;
        const toggle = wrapper?.querySelector(":scope > .folderTreeRow > .folderTreeToggle");
        if (!wrapper || !toggle || toggle.disabled) {
            setTreeToggleState(wrapper, false);
            return false;
        }

        if (!expand) {
            setTreeToggleState(wrapper, false);
            return false;
        }

        if (!keepSiblingsOpen) {
            closeSiblingBranches(wrapper);
            closeAllOtherBranches(wrapper);
        }

        await ensureTreeChildrenLoaded(wrapper, item);
        setTreeToggleState(wrapper, true);
        return true;
    }

    async function toggleTreeExpanded(wrapper, item, options = {}) {
        const children = wrapper?.querySelector(":scope > .folderTreeChildren");
        const isExpanded = !!(wrapper?.classList.contains("expanded") && children && !children.hidden);
        return setTreeExpanded(wrapper, item, !isExpanded, options);
    }

    async function fetchTreeChildren(item) {
        const path = item?.path || "";
        const depth = Number(item?.depth || 0);

        if (depth === 3) {
            const lv3Data = await fetchJson(`/api/explorer/list?path=${encodeURIComponent(path)}`);
            const lv4 = (lv3Data.dirs || []).find((child) => Number(child?.depth || 0) === 4 && String(child?.name || "") === "元データ");
            if (!lv4) return [];
            const lv4Data = await fetchJson(`/api/explorer/list?path=${encodeURIComponent(lv4.path || "")}`);
            return (lv4Data.dirs || []).filter((child) => Number(child?.depth || 0) === UPLOAD_ALLOWED_DEPTH);
        }

        const data = await fetchJson(`/api/explorer/list?path=${encodeURIComponent(path)}`);
        return filterDirsByRule(data.dirs || []);
    }

    function makeTreeNode(item) {
        const wrapper = document.createElement("div");
        wrapper.className = "folderTreeItem";
        wrapper.dataset.path = item.path || "";
        wrapper.dataset.depth = String(item.depth || 0);
        wrapper.dataset.fileCount = String(Number(item.file_count || 0));
        wrapper.style.setProperty("--tree-depth", String(Math.max(0, Number(item.depth || 0))));

        const row = document.createElement("div");
        row.className = "folderTreeRow";

        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "folderTreeToggle";
        toggle.textContent = Number(item.depth || 0) < MAX_DEPTH && item.has_children ? TREE_COLLAPSED_MARK : "";
        toggle.disabled = !(Number(item.depth || 0) < MAX_DEPTH && item.has_children);
        toggle.setAttribute("aria-label", toggle.disabled ? "" : "フォルダを開閉");

        const nodeBtn = document.createElement("button");
        nodeBtn.type = "button";
        nodeBtn.className = "folderTreeNode";
        nodeBtn.innerHTML = renderTreeLabel(item);
        nodeBtn.setAttribute("aria-expanded", "false");

        const children = document.createElement("div");
        children.className = "folderTreeChildren";
        children.hidden = true;

        setTreeToggleState(wrapper, false);

        toggle.addEventListener("click", async (e) => {
            e.preventDefault();
            e.stopPropagation();
            if (toggle.disabled) return;
            try {
                await toggleTreeExpanded(wrapper, item);
            } catch (err) {
                setQueueHint(`ツリー展開失敗: ${String(err?.message || err)}`, "err");
            }
        });

        nodeBtn.addEventListener("click", async () => {
            try {
                if (!toggle.disabled) {
                    await toggleTreeExpanded(wrapper, item);
                }
                await loadFolder(item.path || "");
                highlightSelectedTree(item.path || "");
            } catch (err) {
                setQueueHint(`フォルダ読込失敗: ${String(err?.message || err)}`, "err");
            }
        });

        row.appendChild(toggle);
        row.appendChild(nodeBtn);
        wrapper.appendChild(row);
        wrapper.appendChild(children);
        return wrapper;
    }

    function highlightSelectedTree(path) {
        document.querySelectorAll(".folderTreeItem.selected").forEach((el) => el.classList.remove("selected"));
        const target = document.querySelector(`.folderTreeItem[data-path="${CSS.escape(path || "")}"]`);
        if (target) target.classList.add("selected");
    }

    function visibleTreePathsForFolder(relPath) {
        const parts = splitRelPath(relPath);
        const result = [""];
        const acc = [];
        for (const part of parts) {
            acc.push(part);
            if (acc.length === 4 && part === "元データ") {
                continue;
            }
            result.push(acc.join("/"));
        }
        return result;
    }

    function incrementLoadedTreeCounts(relPath, delta) {
        const diff = Number(delta || 0);
        if (!diff) return;
        const paths = visibleTreePathsForFolder(relPath);
        for (const path of paths) {
            const wrapper = document.querySelector(`.folderTreeItem[data-path="${CSS.escape(path)}"]`);
            if (!wrapper) continue;
            const current = Number(wrapper.dataset.fileCount || 0);
            const next = Math.max(0, current + diff);
            wrapper.dataset.fileCount = String(next);
            const countEl = wrapper.querySelector(":scope > .folderTreeRow .folderTreeCount");
            if (countEl) countEl.textContent = `(${next})`;
        }
    }

    async function loadTreeRoot() {
        const data = await fetchJson("/api/explorer/root");
        folderTree.innerHTML = "";
        const rootNode = makeTreeNode(data.root);
        folderTree.appendChild(rootNode);
        if (data?.root?.has_children) {
            try {
                await setTreeExpanded(rootNode, data.root, true, { keepSiblingsOpen: true });
            } catch (err) {
                setQueueHint(`ツリー初期展開失敗: ${String(err?.message || err)}`, "warn");
            }
        }
    }

    async function loadFolder(path, options = {}) {
        const forceStatus = !!options.forceStatus;
        const silentError = !!options.silentError;
        try {
            const data = await fetchFolderData(path || "", { forceStatus });
            selectedPath = data.current?.path || "";
            currentPath.textContent = formatRelativePath(data.current?.path);
            currentMeta.textContent = `現在階層: Lv${data.current?.depth ?? 0} / 追加: ${data.current?.can_upload ? "可" : "不可"}`;
            setUploadState(!!data.current?.can_upload, Number(data.current?.depth || 0));

            const shouldShowList = !!data.current?.can_upload;
            setListVisible(shouldShowList);
            if (shouldShowList) {
                renderTable(data.dirs, data.files);
            } else {
                currentListDirs = [];
                currentListFiles = [];
            }
        } catch (err) {
            if (!silentError) {
                setQueueHint(`フォルダ読込失敗: ${String(err?.message || err)}`, "err");
            }
        }
    }

    async function refreshVisibleListStatuses() {
        if (!selectedCanUpload || !selectedPath || currentPanel !== "list") return;
        if (folderStatusRefreshBusy) return;
        folderStatusRefreshBusy = true;
        try {
            await loadFolder(selectedPath, { forceStatus: true, silentError: true });
        } finally {
            folderStatusRefreshBusy = false;
        }
    }

    function statusLabel(item) {
        const st = String(item?.status || "");
        if (st === "running") return "処理中";
        if (st === "queued") return "待機中";
        if (st === "completed") return "完了";
        if (st === "skipped") return "差分なし";
        if (st === "error") return "エラー";
        return st || "-";
    }

    function statusClass(item) {
        const st = String(item?.status || "");
        if (st === "running") return "warn";
        if (st === "queued") return "info";
        if (st === "completed") return "ok";
        if (st === "skipped") return "skip";
        if (st === "error") return "err";
        return "";
    }

    function orderLabel(item) {
        const st = String(item?.status || "");
        if (st === "running") return "処理中";
        if (st === "queued") return item?.queue_order ? `#${item.queue_order}` : "待機";
        if (st === "completed") return "完了";
        if (st === "skipped") return "SKIP";
        if (st === "error") return "NG";
        return "-";
    }

    function buildProgressText(item) {
        const parts = [];
        if (item?.stage) parts.push(String(item.stage));
        if (item?.message) parts.push(String(item.message));
        if (Number(item?.attempt_no || 0) > 0) {
            parts.push(`試行=${Number(item.attempt_no || 0)}`);
        }
        if (Number(item?.retry_count || 0) > 0) {
            parts.push(`retry=${Number(item.retry_count || 0)}/${Number(item.max_retry_count || 0)}`);
        }
        return parts.join(" / ") || "-";
    }

    function setQueueVisible(visible) {
        if (visible) {
            queueEmpty.classList.add("hidden");
            queueTableWrap.classList.remove("hidden");
        } else {
            queueTableWrap.classList.add("hidden");
            queueEmpty.classList.remove("hidden");
            queueTableBody.innerHTML = "";
        }
    }

    function switchPanel(panelName) {
        currentPanel = panelName === "queue" ? "queue" : "list";
        const showList = currentPanel === "list";

        tabListBtn.classList.toggle("is-active", showList);
        tabQueueBtn.classList.toggle("is-active", !showList);
        listPanel.classList.toggle("hidden", !showList);
        queuePanel.classList.toggle("hidden", showList);
    }

    async function deleteQueueTask(taskId, fileName) {
        if (!window.confirm(`「${fileName}」をキューから削除します。\n元ファイルとMarkdownも削除します。`)) return;
        try {
            setQueueHint(`削除中: ${fileName}`, "info");
            await fetchJson(`/api/ondemand/queue/${encodeURIComponent(taskId)}/delete`, { method: "POST" });
            setQueueHint(`削除完了: ${fileName}`, "ok");
            await Promise.all([loadFolder(selectedPath, { silentError: true }), loadQueue()]);
        } catch (err) {
            setQueueHint(`削除失敗: ${String(err?.message || err)}`, "err");
            await loadQueue().catch(() => { });
        }
    }

    async function retryQueueTask(taskId, fileName) {
        if (!window.confirm(`「${fileName}」を再試行します。\nMarkdownを削除してキューの最後尾に追加します。`)) return;
        try {
            setQueueHint(`再試行準備中: ${fileName}`, "info");
            await fetchJson(`/api/ondemand/queue/${encodeURIComponent(taskId)}/retry`, { method: "POST" });
            setQueueHint(`再試行をキューに追加しました: ${fileName}`, "ok");
            await loadQueue().catch(() => { });
        } catch (err) {
            setQueueHint(`再試行失敗: ${String(err?.message || err)}`, "err");
            await loadQueue().catch(() => { });
        }
    }

    function renderQueue(items, summary) {
        const list = Array.isArray(items)
            ? items.filter((item) => String(item?.status || "") !== "completed")
            : [];
        const sum = summary || {};
        if (queueTabCount) queueTabCount.textContent = String(list.length);
        queueSummary.textContent = `待機=${Number(sum.queued || 0)} / 処理中=${Number(sum.running || 0)} / 差分なし=${Number(sum.skipped || 0)} / エラー=${Number(sum.error || 0)}`;

        if (!list.length) {
            setQueueVisible(false);
            return;
        }

        setQueueVisible(true);
        queueTableBody.innerHTML = "";

        for (const item of list) {
            const isError = String(item?.status || "") === "error";
            const taskId = String(item?.id || "");
            const fileName = String(item?.source_display_name || "-");
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${escapeHtml(orderLabel(item))}</td>
                <td><span class="queueStatus ${escapeHtml(statusClass(item))}">${escapeHtml(statusLabel(item))}</span></td>
                <td class="queueCellWrap">${escapeHtml(item.folder_display || "-")}</td>
                <td class="queueCellWrap">${renderNameCell(item.source_display_name || "-", false)}</td>
                <td class="queueCellWrap">${escapeHtml(item.markdown_name || "-")}</td>
                <td class="queueCellWrap queueProgressCell">${escapeHtml(buildProgressText(item))}</td>
                <td>${escapeHtml(item.updated_at || "-")}</td>
                <td class="queueActionCell">${isError ? `
                    <button type="button" class="queueRetryBtn" data-task-id="${escapeHtml(taskId)}" title="Markdownを削除してキュー最後尾に再追加">再試行</button>
                    <button type="button" class="queueDeleteBtn" data-task-id="${escapeHtml(taskId)}" title="元ファイルとMarkdownを削除">削除</button>
                ` : "-"}</td>
            `;
            if (isError && taskId) {
                const retryBtn = tr.querySelector(".queueRetryBtn");
                const deleteBtn = tr.querySelector(".queueDeleteBtn");
                if (retryBtn) retryBtn.addEventListener("click", () => retryQueueTask(taskId, fileName));
                if (deleteBtn) deleteBtn.addEventListener("click", () => deleteQueueTask(taskId, fileName));
            }
            queueTableBody.appendChild(tr);
        }
    }

    async function loadQueue() {
        try {
            const data = await fetchJson("/api/ondemand/queue?limit=200", { cache: "no-store" });
            renderQueue(data.items || [], data.summary || {});
            if (!queueHint.classList.contains("err")) {
                setQueueHint("フォルダ横断でフェアに1件ずつ処理します。", "");
            }
            await refreshVisibleListStatuses();
        } catch (err) {
            setQueueHint(`キュー取得失敗: ${String(err?.message || err)}`, "err");
        }
    }

    function startQueuePolling() {
        if (queuePollTimer) clearInterval(queuePollTimer);
        queuePollTimer = setInterval(() => {
            loadQueue().catch(() => { });
        }, 2000);
    }

    function splitUploadFiles(files) {
        const supported = [];
        const blockedUnsupported = [];
        const blockedOversize = [];
        for (const file of Array.from(files || [])) {
            if (!isSupportedFileName(file?.name || "")) {
                blockedUnsupported.push(file);
                continue;
            }
            if (Number(file?.size || 0) > MAX_UPLOAD_FILE_SIZE_BYTES) {
                blockedOversize.push(file);
                continue;
            }
            supported.push(file);
        }
        return { supported, blockedUnsupported, blockedOversize };
    }

    function buildBlockedFilesMessage(blocked) {
        if (!blocked.length) return "";
        const sample = blocked.slice(0, 3).map((file) => String(file?.name || "")).filter(Boolean).join(", ");
        if (blocked.length <= 3) return sample;
        return `${sample} ほか${blocked.length - 3}件`;
    }

    async function deleteFile(item) {
        const filePath = String(item?.path || "");
        const fileName = String(item?.name || "");
        if (!filePath || !fileName) {
            setQueueHint("削除対象ファイルを判定できません。", "err");
            return;
        }
        if (!window.confirm(`「${fileName}」を削除します。
対応するMarkdownも削除します。`)) {
            return;
        }

        deleteBusyPaths.add(filePath);
        renderTable(currentListDirs, currentListFiles);
        try {
            setQueueHint(`削除中: ${fileName}`, "info");
            const data = await fetchJson('/api/explorer/delete', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ path: filePath }),
            });
            const deleted = data.deleted || {};
            const parts = [
                `ファイル削除=1`,
                `Markdown削除=${deleted.markdown_deleted ? 1 : 0}`,
            ];
            const queueRemovedCount = Number(deleted.queue_removed_count || deleted.queue_paused_count || 0);
            if (queueRemovedCount > 0) {
                parts.push(`キュー削除=${queueRemovedCount}`);
            }
            setQueueHint(parts.join(' / '), 'ok');
            incrementLoadedTreeCounts(selectedPath, -1);
            await Promise.all([loadFolder(selectedPath), loadQueue()]);
        } catch (err) {
            setQueueHint(`削除失敗: ${String(err?.message || err)}`, 'err');
            await Promise.all([loadFolder(selectedPath), loadQueue()]).catch(() => { });
        } finally {
            deleteBusyPaths.delete(filePath);
            if (selectedCanUpload) {
                renderTable(currentListDirs, currentListFiles);
            }
        }
    }

    async function uploadFiles(files) {
        if (!files || files.length === 0) return;

        if (!selectedCanUpload) {
            setQueueHint(`Lv${UPLOAD_ALLOWED_DEPTH}のフォルダでのみアップロードできます。`, "err");
            return;
        }

        const { supported, blockedUnsupported, blockedOversize } = splitUploadFiles(files);
        if (!supported.length) {
            const reasons = [];
            if (blockedUnsupported.length) reasons.push(`非対応拡張子=${blockedUnsupported.length}`);
            if (blockedOversize.length) reasons.push(`100MB超過=${blockedOversize.length}`);
            setQueueHint(`追加できません。${reasons.join(" / ")} / 対応拡張子: ${formatAllowedExtensions()}`, "err");
            return;
        }

        const fd = new FormData();
        fd.append("path", selectedPath);
        for (const file of supported) {
            fd.append("files", file, file.name);
        }

        try {
            const excludedParts = [];
            if (blockedUnsupported.length) excludedParts.push(`非対応 ${blockedUnsupported.length} 件`);
            if (blockedOversize.length) excludedParts.push(`100MB超過 ${blockedOversize.length} 件`);
            if (excludedParts.length) {
                setQueueHint(`${excludedParts.join(" / ")} を除外してアップロードします。`, "warn");
            } else {
                setQueueHint(`アップロード中: ${supported.length}件`, "info");
            }

            const data = await fetchJson("/api/explorer/upload", {
                method: "POST",
                body: fd,
            });

            const savedCount = Array.isArray(data.saved) ? data.saved.length : 0;
            const skippedCount = Array.isArray(data.skipped) ? data.skipped.length : 0;
            const errorCount = Array.isArray(data.errors) ? data.errors.length : 0;
            const queueCount = Array.isArray(data.queue_items) ? data.queue_items.length : 0;
            const queueErrorCount = Array.isArray(data.queue_errors) ? data.queue_errors.length : 0;

            const parts = [
                `保存=${savedCount}`,
                `キュー投入=${queueCount}`,
                `保存スキップ=${skippedCount}`,
                `保存エラー=${errorCount}`,
            ];
            if (queueErrorCount) parts.push(`キューエラー=${queueErrorCount}`);
            if (blockedUnsupported.length) parts.push(`対象外除外=${blockedUnsupported.length}`);
            if (blockedOversize.length) parts.push(`容量超過除外=${blockedOversize.length}`);
            const note = parts.join(" / ");
            setQueueHint(note, queueErrorCount || errorCount ? "warn" : (blockedUnsupported.length || blockedOversize.length || skippedCount ? "warn" : "ok"));

            if (savedCount > 0) incrementLoadedTreeCounts(selectedPath, savedCount);
            switchPanel("queue");
            await Promise.all([loadFolder(selectedPath), loadQueue()]);
        } catch (err) {
            const blockedParts = [];
            if (blockedUnsupported.length) blockedParts.push(`非対応除外: ${buildBlockedFilesMessage(blockedUnsupported)}`);
            if (blockedOversize.length) blockedParts.push(`100MB超過除外: ${buildBlockedFilesMessage(blockedOversize)}`);
            const blockedMsg = blockedParts.length ? ` / ${blockedParts.join(" / ")}` : "";
            setQueueHint(`アップロード失敗: ${String(err?.message || err)}${blockedMsg}`, "err");
            switchPanel("queue");
        }
    }

    dropZone.addEventListener("dragover", (e) => {
        if (!selectedCanUpload) return;
        e.preventDefault();
        dropZone.classList.add("dragover");
    });

    dropZone.addEventListener("dragleave", () => {
        dropZone.classList.remove("dragover");
    });

    dropZone.addEventListener("drop", async (e) => {
        if (!selectedCanUpload) return;
        e.preventDefault();
        dropZone.classList.remove("dragover");
        const files = Array.from(e.dataTransfer.files || []);
        await uploadFiles(files);
    });

    fileSelectBtn.addEventListener("click", () => {
        if (!selectedCanUpload) return;
        fileInput.click();
    });

    fileInput.addEventListener("change", async (e) => {
        const files = Array.from(e.target.files || []);
        await uploadFiles(files);
        fileInput.value = "";
    });

    refreshBtn.addEventListener("click", async () => {
        try {
            await Promise.all([loadFolder(selectedPath || ""), loadQueue()]);
        } catch {
            // no-op
        }
    });

    tabListBtn.addEventListener("click", async () => {
        switchPanel("list");
        await refreshVisibleListStatuses();
    });

    tabQueueBtn.addEventListener("click", () => {
        switchPanel("queue");
    });

    for (const btn of listSortButtons) {
        btn.addEventListener("click", () => {
            setListSort(btn.dataset.sortKey || "");
        });
    }

    window.addEventListener("resize", syncOnDemandSidebarLayout);

    (async () => {
        try {
            syncOnDemandSidebarLayout();
            renderAllowedExtensions();
            applyListSortIndicators();
            switchPanel("list");

            await loadTreeRoot();
            await Promise.all([loadFolder(""), loadQueue()]);
            highlightSelectedTree("");
            syncOnDemandSidebarLayout();
            startQueuePolling();
        } catch (err) {
            setQueueHint(`初期化失敗: ${String(err?.message || err)}`, "err");
        }
    })();
})();