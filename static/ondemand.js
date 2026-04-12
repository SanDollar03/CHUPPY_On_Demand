(() => {
    function loadOnDemandConfig() {
        const defaults = {
            rootPath: "",
            maxDepth: 5,
            uploadAllowedDepth: 5,
        };

        const el = document.getElementById("ondemand-config");
        if (!el) return defaults;

        try {
            return {
                ...defaults,
                ...(JSON.parse(el.textContent || "{}") || {}),
            };
        } catch (err) {
            console.error("ONDemandConfig parse error:", err);
            return defaults;
        }
    }

    const cfg = loadOnDemandConfig();
    const MAX_DEPTH = Number(cfg.maxDepth || 5);
    const UPLOAD_ALLOWED_DEPTH = Number(cfg.uploadAllowedDepth || 5);

    const folderTree = document.getElementById("folderTree");
    const currentPath = document.getElementById("currentPath");
    const currentMeta = document.getElementById("currentMeta");
    const knowledgeLabel = document.getElementById("knowledgeLabel");

    const dropZone = document.getElementById("dropZone");
    const dropZoneSub = document.getElementById("dropZoneSub");
    const fileInput = document.getElementById("fileInput");
    const fileSelectBtn = document.getElementById("fileSelectBtn");
    const refreshBtn = document.getElementById("refreshBtn");

    const listEmpty = document.getElementById("listEmpty");
    const listTableWrap = document.getElementById("listTableWrap");
    const listState = document.getElementById("listState");
    const fileTableBody = document.getElementById("fileTableBody");

    const queueSummary = document.getElementById("queueSummary");
    const queueHint = document.getElementById("queueHint");
    const queueEmpty = document.getElementById("queueEmpty");
    const queueTableWrap = document.getElementById("queueTableWrap");
    const queueTableBody = document.getElementById("queueTableBody");

    let selectedPath = "";
    let selectedDepth = 0;
    let selectedCanUpload = false;
    let queuePollTimer = null;

    const datasetState = {
        loaded: false,
        items: [],
        error: "",
    };

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
            .replace(/"/g, "&quot;")
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

    function formatRelativePath(relPath) {
        const rel = String(relPath || "").replace(/^\/+|\/+$/g, "");
        return rel || ".";
    }

    function buildKnowledgeNameFromPath(relPath) {
        const norm = String(relPath || "").replace(/^\/+|\/+$/g, "");
        const parts = norm ? norm.split("/").filter(Boolean) : [];
        if (parts.length !== UPLOAD_ALLOWED_DEPTH) return "";
        const filtered = parts.filter((part) => part !== "元データ");
        if (!filtered.length) return "";
        return `Chu_${filtered.join("_")}`;
    }

    function setKnowledgeLabel(text, kind = "") {
        if (!knowledgeLabel) return;
        knowledgeLabel.textContent = text || "-";
        knowledgeLabel.className = `ondemandKnowledgeLabel${kind ? ` ${kind}` : ""}`;
    }

    async function fetchJson(url, options = {}) {
        const res = await fetch(url, options);
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.ok === false) {
            throw new Error(data.error || `HTTP ${res.status}`);
        }
        return data;
    }

    async function ensureDatasetsLoaded() {
        if (datasetState.loaded) return datasetState.items;
        const data = await fetchJson("/api/datasets", { cache: "no-store" });
        datasetState.loaded = true;
        datasetState.items = Array.isArray(data.items) ? data.items : [];
        datasetState.error = "";
        return datasetState.items;
    }

    async function updateKnowledgeLabel(relPath, canUpload) {
        if (!canUpload) {
            setKnowledgeLabel(`Lv${UPLOAD_ALLOWED_DEPTH}フォルダを選択してください`);
            return;
        }

        const knowledgeName = buildKnowledgeNameFromPath(relPath);
        if (!knowledgeName) {
            setKnowledgeLabel("ナレッジ名を判定できません。", "err");
            return;
        }

        try {
            const items = await ensureDatasetsLoaded();
            const found = items.find((item) => String(item?.name || "") === knowledgeName);
            if (found) {
                setKnowledgeLabel(found.name, "ok");
            } else {
                setKnowledgeLabel("ナレッジが存在しません。管理者に問い合わせてください", "err");
            }
        } catch (err) {
            datasetState.loaded = false;
            datasetState.items = [];
            datasetState.error = String(err?.message || err || "");
            setKnowledgeLabel("ナレッジ一覧の取得に失敗しました。", "err");
            setQueueHint(`ナレッジ一覧取得失敗: ${datasetState.error}`, "err");
        }
    }

    function setUploadState(canUpload, depth) {
        selectedCanUpload = !!canUpload;
        selectedDepth = Number(depth || 0);

        if (selectedCanUpload) {
            dropZone.classList.remove("disabled");
            fileSelectBtn.disabled = false;
            dropZoneSub.textContent = `Lv${UPLOAD_ALLOWED_DEPTH}フォルダです。ファイル追加できます。`;
        } else {
            dropZone.classList.add("disabled");
            fileSelectBtn.disabled = true;
            dropZoneSub.textContent = `Lv${UPLOAD_ALLOWED_DEPTH}フォルダ選択時のみファイル追加できます（現在: Lv${selectedDepth}）`;
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

    function renderTable(dirs, files) {
        fileTableBody.innerHTML = "";
        const hasDirs = Array.isArray(dirs) && dirs.length > 0;
        const hasFiles = Array.isArray(files) && files.length > 0;

        if (!hasDirs && !hasFiles) {
            const tr = document.createElement("tr");
            tr.innerHTML = `<td colspan="4" class="empty-cell">ファイルはありません。</td>`;
            fileTableBody.appendChild(tr);
            return;
        }

        for (const d of dirs || []) {
            const tr = document.createElement("tr");
            tr.className = "clickable-row";
            tr.innerHTML = `
                <td>📁</td>
                <td>${escapeHtml(d.name)}</td>
                <td>${escapeHtml(d.mtime || "-")}</td>
                <td>-</td>
            `;
            tr.addEventListener("click", () => loadFolder(d.path || ""));
            fileTableBody.appendChild(tr);
        }

        for (const f of files || []) {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>📄</td>
                <td>${escapeHtml(f.name)}</td>
                <td>${escapeHtml(f.mtime || "-")}</td>
                <td>${escapeHtml(formatBytes(f.size_bytes))}</td>
            `;
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
            sib.classList.remove("expanded");
            const toggle = sib.querySelector(":scope > .folderTreeRow > .folderTreeToggle");
            const children = sib.querySelector(":scope > .folderTreeChildren");
            if (toggle && !toggle.disabled) toggle.textContent = "+";
            if (children) children.hidden = true;
        });
    }

    function closeAllOtherBranches(currentWrapper) {
        const expanded = folderTree.querySelectorAll(".folderTreeItem.expanded");
        expanded.forEach((item) => {
            if (item === currentWrapper) return;
            if (item.contains(currentWrapper)) return;
            if (currentWrapper.contains(item)) return;
            item.classList.remove("expanded");
            const toggle = item.querySelector(":scope > .folderTreeRow > .folderTreeToggle");
            const children = item.querySelector(":scope > .folderTreeChildren");
            if (toggle && !toggle.disabled) toggle.textContent = "+";
            if (children) children.hidden = true;
        });
    }

    function makeTreeNode(item) {
        const wrapper = document.createElement("div");
        wrapper.className = "folderTreeItem";
        wrapper.dataset.path = item.path || "";
        wrapper.dataset.depth = String(item.depth || 0);

        const row = document.createElement("div");
        row.className = "folderTreeRow";

        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "folderTreeToggle";
        toggle.textContent = item.depth < MAX_DEPTH && item.has_children ? "+" : "";
        toggle.disabled = !(item.depth < MAX_DEPTH && item.has_children);

        const nodeBtn = document.createElement("button");
        nodeBtn.type = "button";
        nodeBtn.className = "folderTreeNode";
        nodeBtn.innerHTML = `
            <span class="folderTreeName">${escapeHtml(item.name || "/")}</span>
            <span class="folderTreeLv">Lv${escapeHtml(item.depth)}</span>
        `;

        const children = document.createElement("div");
        children.className = "folderTreeChildren";
        children.hidden = true;

        toggle.addEventListener("click", async (e) => {
            e.stopPropagation();
            if (toggle.disabled) return;

            const expanded = wrapper.classList.contains("expanded");
            if (expanded) {
                wrapper.classList.remove("expanded");
                children.hidden = true;
                toggle.textContent = "+";
                return;
            }

            try {
                closeSiblingBranches(wrapper);
                closeAllOtherBranches(wrapper);

                if (!children.dataset.loaded) {
                    const data = await fetchJson(`/api/explorer/list?path=${encodeURIComponent(item.path || "")}`);
                    const filteredDirs = filterDirsByRule(data.dirs || []);
                    children.innerHTML = "";
                    for (const child of filteredDirs) {
                        if (Number(child.depth) <= MAX_DEPTH) children.appendChild(makeTreeNode(child));
                    }
                    children.dataset.loaded = "1";
                }

                wrapper.classList.add("expanded");
                children.hidden = false;
                toggle.textContent = "-";
            } catch (err) {
                setQueueHint(`ツリー展開失敗: ${String(err?.message || err)}`, "err");
            }
        });

        nodeBtn.addEventListener("click", async () => {
            await loadFolder(item.path || "");
            highlightSelectedTree(item.path || "");
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

    async function loadTreeRoot() {
        const data = await fetchJson("/api/explorer/root");
        folderTree.innerHTML = "";
        folderTree.appendChild(makeTreeNode(data.root));
    }

    async function loadFolder(path) {
        try {
            const data = await fetchJson(`/api/explorer/list?path=${encodeURIComponent(path || "")}`);
            selectedPath = data.current?.path || "";
            currentPath.textContent = formatRelativePath(data.current?.path);
            currentMeta.textContent = `現在階層: Lv${data.current?.depth ?? 0} / 追加: ${data.current?.can_upload ? "可" : "不可"}`;
            setUploadState(!!data.current?.can_upload, Number(data.current?.depth || 0));
            await updateKnowledgeLabel(data.current?.path || "", !!data.current?.can_upload);

            const shouldShowList = !!data.current?.can_upload;
            setListVisible(shouldShowList);
            if (shouldShowList) renderTable(data.dirs, data.files);
        } catch (err) {
            setQueueHint(`フォルダ読込失敗: ${String(err?.message || err)}`, "err");
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
        if (Number(item?.total_segments || 0) > 0) {
            parts.push(`segments=${Number(item.completed_segments || 0)}/${Number(item.total_segments || 0)}`);
        }
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

    function renderQueue(items, summary) {
        const list = Array.isArray(items)
            ? items.filter((item) => String(item?.status || "") !== "completed")
            : [];
        const sum = summary || {};
        queueSummary.textContent = `待機=${Number(sum.queued || 0)} / 処理中=${Number(sum.running || 0)} / 差分なし=${Number(sum.skipped || 0)} / エラー=${Number(sum.error || 0)}`;

        if (!list.length) {
            setQueueVisible(false);
            return;
        }

        setQueueVisible(true);
        queueTableBody.innerHTML = "";

        for (const item of list) {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${escapeHtml(orderLabel(item))}</td>
                <td><span class="queueStatus ${escapeHtml(statusClass(item))}">${escapeHtml(statusLabel(item))}</span></td>
                <td class="queueCellWrap">${escapeHtml(item.folder_display || "-")}</td>
                <td class="queueCellWrap">${escapeHtml(item.source_display_name || "-")}</td>
                <td class="queueCellWrap">${escapeHtml(item.dataset_name || "-")}</td>
                <td class="queueCellWrap queueProgressCell">${escapeHtml(buildProgressText(item))}</td>
                <td>${escapeHtml(item.updated_at || "-")}</td>
            `;
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
        } catch (err) {
            setQueueHint(`キュー取得失敗: ${String(err?.message || err)}`, "err");
        }
    }

    function startQueuePolling() {
        if (queuePollTimer) clearInterval(queuePollTimer);
        queuePollTimer = setInterval(() => {
            loadQueue().catch(() => {});
        }, 2000);
    }

    async function uploadFiles(files) {
        if (!files || files.length === 0) return;

        if (!selectedCanUpload) {
            setQueueHint(`Lv${UPLOAD_ALLOWED_DEPTH}のフォルダでのみアップロードできます。`, "err");
            return;
        }

        const fd = new FormData();
        fd.append("path", selectedPath);
        for (const file of files) {
            fd.append("files", file, file.name);
        }

        try {
            setQueueHint(`アップロード中: ${files.length}件`, "info");
            const data = await fetchJson("/api/explorer/upload", {
                method: "POST",
                body: fd,
            });

            const savedCount = Array.isArray(data.saved) ? data.saved.length : 0;
            const skippedCount = Array.isArray(data.skipped) ? data.skipped.length : 0;
            const errorCount = Array.isArray(data.errors) ? data.errors.length : 0;
            const queueCount = Array.isArray(data.queue_items) ? data.queue_items.length : 0;
            const queueErrorCount = Array.isArray(data.queue_errors) ? data.queue_errors.length : 0;

            const note = `保存=${savedCount} / キュー投入=${queueCount} / 保存スキップ=${skippedCount} / 保存エラー=${errorCount} / キューエラー=${queueErrorCount}`;
            setQueueHint(note, queueErrorCount || errorCount ? "warn" : "ok");

            await Promise.all([loadFolder(selectedPath), loadQueue()]);
        } catch (err) {
            setQueueHint(`アップロード失敗: ${String(err?.message || err)}`, "err");
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
        await Promise.all([loadFolder(selectedPath || ""), loadQueue()]);
    });

    window.addEventListener("resize", syncOnDemandSidebarLayout);

    (async () => {
        try {
            syncOnDemandSidebarLayout();
            try {
                await ensureDatasetsLoaded();
            } catch (err) {
                setQueueHint(`ナレッジ一覧先読み失敗: ${String(err?.message || err)}`, "warn");
            }

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
