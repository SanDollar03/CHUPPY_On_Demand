# ondemand_queue.py
# -*- coding: utf-8 -*-

from ondemand_core import *


class OnDemandQueueManager:
    def __init__(self):
        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._started = False
        self._thread: Optional[threading.Thread] = None
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._task_order: List[str] = []
        self._folder_queues: Dict[str, deque] = {}
        self._ready_folders: deque = deque()
        self._running_task_id: str = ""
        self._running_folder: str = ""
        self._active_doc_keys: Dict[str, str] = {}
        self._handled_source_signatures: Dict[str, Dict[str, Any]] = {}
        self._handled_source_order: deque = deque()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._thread = threading.Thread(target=self._worker_loop, name="ondemand-queue-worker", daemon=True)
            self._thread.start()
            self._started = True

    def enqueue_saved_file(
        self,
        folder_rel_path: str,
        folder_abs_path: str,
        source_abs_path: str,
        source_rel_path: str,
        source_saved_name: str,
        source_original_name: str,
        source_signature: str = "",
        dataset_hint: Optional[Dict[str, Any]] = None,
        queue_message: str = "",
    ) -> Dict[str, Any]:
        folder_rel = (folder_rel_path or "").strip().replace("\\", "/").strip("/")
        source_signature = (source_signature or build_source_signature(source_abs_path, source_rel_path)).strip()
        dataset_name = build_ondemand_dataset_name(folder_rel)
        dataset = dict(dataset_hint) if isinstance(dataset_hint, dict) and dataset_hint else None
        message = ""
        source_name_for_ext = source_original_name or source_saved_name
        source_ext = os.path.splitext(source_name_for_ext or "")[1].lower()

        if not is_allowed_extension(source_ext):
            message = f"非対応拡張子です: {source_ext or '(拡張子なし)'}"
        elif not API_BASE or not API_KEY:
            message = "生成AI API設定が未完了です。"
        elif not DATASET_API_BASE or not DATASET_API_KEY:
            message = "ナレッジAPI設定が未完了です。"
        elif not dataset_name:
            message = "ナレッジ名を判定できません。"
        elif not dataset:
            try:
                dataset = find_dataset_by_name(dataset_name)
            except Exception as e:
                dataset = None
                message = safe_err(str(e)) or "ナレッジ一覧の取得に失敗しました。"
            if not dataset and not message:
                message = "ナレッジが存在しません。管理者に問い合わせてください"

        md_abs_path = ""
        md_rel_path = ""
        md_name = ""
        try:
            md_abs_path, md_rel_path, md_name = build_ondemand_markdown_path(folder_rel, source_original_name)
        except Exception as e:
            if not message:
                message = safe_err(str(e))

        doc_key = build_ondemand_doc_key((dataset or {}).get("id") or "", dataset_name, md_name)

        with self._cv:
            handled = self._handled_source_signatures.get(source_signature) if source_signature else None
            if handled:
                return dict(handled.get("snapshot") or {})

            existing = self._find_task_by_source_signature_locked(source_signature)
            if existing:
                existing_status = str(existing.get("status") or "")
                existing_terminal = bool(existing.get("terminal"))
                if existing_terminal and existing_status == "error" and dataset and md_abs_path:
                    revived = self._revive_failed_task_locked(
                        existing,
                        dataset=dataset,
                        md_abs_path=md_abs_path,
                        md_rel_path=md_rel_path,
                        md_name=md_name,
                        doc_key=doc_key,
                        queue_message=queue_message or "前回エラーのためリトライをリセットして再投入しました。",
                    )
                    self._cv.notify_all()
                    return self._public_task_snapshot(revived, self._queue_order_for_task_locked(revived.get("id") or ""))

                if existing_terminal and existing_status == "error" and message:
                    existing["message"] = message
                    existing["stage"] = "受付不可"
                    existing["updated_at"] = now_label()
                    existing["last_error"] = message
                return self._public_task_snapshot(existing, self._queue_order_for_task_locked(existing.get("id") or ""))

            if doc_key:
                existing_task_id = self._active_doc_keys.get(doc_key) or ""
                existing_task = self._tasks.get(existing_task_id) if existing_task_id else None
                if existing_task:
                    return self._public_task_snapshot(existing_task, self._queue_order_for_task_locked(existing_task_id))

            task_id = uuid.uuid4().hex
            now = now_label()
            task = {
                "id": task_id,
                "folder_rel_path": folder_rel,
                "folder_abs_path": folder_abs_path,
                "folder_display": folder_rel or ".",
                "source_abs_path": source_abs_path,
                "source_rel_path": source_rel_path,
                "source_saved_name": source_saved_name,
                "source_original_name": source_original_name,
                "source_display_name": source_original_name or source_saved_name,
                "source_signature": source_signature,
                "dataset_name": dataset_name,
                "dataset_id": (dataset or {}).get("id") or "",
                "markdown_abs_path": md_abs_path,
                "markdown_rel_path": md_rel_path,
                "markdown_name": md_name,
                "doc_key": doc_key,
                "status": "queued" if dataset and md_abs_path else "error",
                "stage": "順番待ち" if dataset and md_abs_path else "受付不可",
                "message": (queue_message or "アップロード待ちキューに追加しました。") if dataset and md_abs_path else message,
                "attempt_no": 0,
                "retry_count": 0,
                "max_retry_count": ONDEMAND_QUEUE_MAX_RETRIES,
                "created_at": now,
                "updated_at": now,
                "started_at": "",
                "finished_at": "",
                "terminal": not bool(dataset and md_abs_path),
                "doc_id": "",
                "batch": "",
                "indexing_status": "",
                "completed_segments": 0,
                "total_segments": 0,
                "last_error": message if message else "",
                "markdown_written": False,
                "queue_order": None,
                "result": "pending" if dataset and md_abs_path else "error",
            }

            self._tasks[task_id] = task
            self._task_order.append(task_id)
            self._prune_locked()
            if task["status"] == "queued":
                if doc_key:
                    self._active_doc_keys[doc_key] = task_id
                fq = self._folder_queues.setdefault(folder_rel, deque())
                fq.append(task_id)
                self._ensure_folder_ready_locked(folder_rel)
                self._cv.notify_all()
                append_ondemand_queue_log("task_queued", task)
            else:
                append_ondemand_queue_log("task_rejected", task)

            return self._public_task_snapshot(task, queue_order=None)

    def get_snapshot(self, limit: int = 200, hide_completed: bool = True) -> Dict[str, Any]:
        with self._lock:
            queue_order_map = self._build_queue_order_map_locked()
            running_id = self._running_task_id
            items: List[Dict[str, Any]] = []
            summary_all = {
                "queued": 0,
                "running": 0,
                "completed": 0,
                "skipped": 0,
                "error": 0,
                "total": len(self._tasks),
            }
            summary_visible = {
                "queued": 0,
                "running": 0,
                "completed": 0,
                "skipped": 0,
                "error": 0,
                "total": 0,
            }

            for seq, task_id in enumerate(self._task_order, start=1):
                task = self._tasks.get(task_id)
                if not task:
                    continue
                st = str(task.get("status") or "")
                if st in summary_all:
                    summary_all[st] += 1
                if hide_completed and st == "completed":
                    continue
                if st in summary_visible:
                    summary_visible[st] += 1
                qord = 0 if task_id == running_id else queue_order_map.get(task_id)
                item = self._public_task_snapshot(task, qord)
                item["_seq"] = seq
                items.append(item)

            summary_visible["total"] = len(items)
            items.sort(key=self._sort_key)
            for item in items:
                item.pop("_seq", None)
            if limit > 0:
                items = items[:limit]

            return {
                "summary": summary_visible,
                "summary_all": summary_all,
                "items": items,
            }

    def get_task_snapshot_by_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            return self._public_task_snapshot(task, self._queue_order_for_task_locked(task_id))

    def get_task_snapshot_by_source_signature(self, source_signature: str) -> Optional[Dict[str, Any]]:
        sig = (source_signature or "").strip()
        if not sig:
            return None
        with self._lock:
            handled = self._handled_source_signatures.get(sig)
            if handled:
                return dict(handled.get("snapshot") or {})
            task = self._find_task_by_source_signature_locked(sig)
            if not task:
                return None
            if bool(task.get("terminal")) and str(task.get("status") or "") == "error":
                return None
            return self._public_task_snapshot(task, self._queue_order_for_task_locked(task.get("id") or ""))

    def get_latest_task_snapshots_by_source_rel_paths(self, source_rel_paths: List[str]) -> Dict[str, Dict[str, Any]]:
        wanted: Dict[str, str] = {}
        for rel_path in (source_rel_paths or []):
            rel_key = normalize_name_key(str(rel_path or "").replace("\\", "/"))
            if rel_key:
                wanted[rel_key] = str(rel_path or "")
        if not wanted:
            return {}

        with self._lock:
            out: Dict[str, Dict[str, Any]] = {}
            queue_order_map = self._build_queue_order_map_locked()

            for task_id in reversed(self._task_order):
                task = self._tasks.get(task_id)
                if not task:
                    continue
                rel_key = normalize_name_key(str(task.get("source_rel_path") or "").replace("\\", "/"))
                if not rel_key or rel_key not in wanted or rel_key in out:
                    continue
                qord = 0 if task_id == self._running_task_id else queue_order_map.get(task_id)
                out[rel_key] = self._public_task_snapshot(task, qord)
                if len(out) >= len(wanted):
                    break

            if len(out) < len(wanted):
                for handled in reversed(list(self._handled_source_signatures.values())):
                    snapshot = dict((handled or {}).get("snapshot") or {})
                    rel_key = normalize_name_key(str(snapshot.get("source_rel_path") or "").replace("\\", "/"))
                    if not rel_key or rel_key not in wanted or rel_key in out:
                        continue
                    out[rel_key] = snapshot
                    if len(out) >= len(wanted):
                        break

            return out

    def pause_delete_for_source(self, source_rel_path: str) -> Dict[str, Any]:
        rel_key = normalize_name_key((source_rel_path or "").replace("\\", "/"))
        if not rel_key:
            return {"ok": False, "error": "削除対象ファイルを判定できません。"}

        paused_task_ids: List[str] = []
        with self._cv:
            for task_id in list(self._task_order):
                task = self._tasks.get(task_id)
                if not task:
                    continue
                task_rel_key = normalize_name_key(str(task.get("source_rel_path") or "").replace("\\", "/"))
                if task_rel_key != rel_key:
                    continue
                if task_id == self._running_task_id or (str(task.get("status") or "") == "running" and not bool(task.get("terminal"))):
                    return {"ok": False, "error": "このファイルは現在処理中のため削除できません。"}
                if str(task.get("status") or "") == "queued" and not bool(task.get("terminal")):
                    self._remove_task_from_folder_queues_locked(task_id)
                    task["status"] = "paused_delete"
                    task["stage"] = "削除準備"
                    task["message"] = "ファイル削除のため一時停止しています。"
                    task["updated_at"] = now_label()
                    paused_task_ids.append(task_id)
            if paused_task_ids:
                self._prune_ready_folders_locked()
                self._cv.notify_all()
        return {"ok": True, "source_rel_path": str(source_rel_path or ""), "paused_task_ids": paused_task_ids, "paused_count": len(paused_task_ids)}

    def restore_paused_delete(self, pause_info: Optional[Dict[str, Any]], message: str = "") -> None:
        paused_task_ids = list((pause_info or {}).get("paused_task_ids") or [])
        if not paused_task_ids:
            return
        with self._cv:
            for task_id in paused_task_ids:
                task = self._tasks.get(task_id)
                if not task or bool(task.get("terminal")):
                    continue
                if str(task.get("status") or "") != "paused_delete":
                    continue
                task["status"] = "queued"
                task["stage"] = "順番待ち"
                task["message"] = message or "削除処理を取り消したためキューへ戻しました。"
                task["updated_at"] = now_label()
                folder = str(task.get("folder_rel_path") or "")
                fq = self._folder_queues.setdefault(folder, deque())
                if task_id not in fq:
                    fq.appendleft(task_id)
                self._ensure_folder_ready_locked(folder)
                append_ondemand_queue_log("task_delete_restore", task)
            self._prune_ready_folders_locked()
            self._cv.notify_all()

    def finalize_paused_delete(self, pause_info: Optional[Dict[str, Any]], message: str = "") -> Dict[str, int]:
        source_rel_path = str((pause_info or {}).get("source_rel_path") or "")
        if not source_rel_path:
            paused_task_ids = list((pause_info or {}).get("paused_task_ids") or [])
            for task_id in paused_task_ids:
                task = self._tasks.get(task_id)
                if task:
                    source_rel_path = str(task.get("source_rel_path") or "")
                    if source_rel_path:
                        break
        if not source_rel_path:
            return {"removed_count": 0, "handled_removed_count": 0}

        with self._cv:
            removed = self._purge_source_rel_path_locked(source_rel_path, log_status="deleted")
            self._cv.notify_all()
            return removed

    def remember_handled_source_signature(self, source_signature: str, status: str, stage: str, message: str, result: str) -> None:
        snapshot = {
            "id": "",
            "folder_rel_path": "",
            "folder_display": "",
            "source_display_name": "",
            "source_saved_name": "",
            "source_rel_path": "",
            "dataset_name": "",
            "dataset_id": "",
            "markdown_name": "",
            "markdown_rel_path": "",
            "status": status,
            "stage": stage,
            "message": message,
            "attempt_no": 0,
            "retry_count": 0,
            "max_retry_count": ONDEMAND_QUEUE_MAX_RETRIES,
            "created_at": "",
            "updated_at": now_label(),
            "started_at": "",
            "finished_at": now_label(),
            "terminal": True,
            "doc_id": "",
            "batch": "",
            "indexing_status": "",
            "completed_segments": 0,
            "total_segments": 0,
            "queue_order": None,
            "last_error": "",
            "result": result,
        }
        with self._lock:
            self._remember_handled_source_signature_locked(source_signature, snapshot)

    def _sort_key(self, item: Dict[str, Any]):
        status = str(item.get("status") or "")
        queue_order = item.get("queue_order")
        seq = int(item.get("_seq") or 0)
        if status == "running":
            return (0, 0, 0)
        if status == "queued":
            return (1, int(queue_order or 999999), 0)
        return (2, 999999, -seq)

    def _public_task_snapshot(self, task: Dict[str, Any], queue_order: Optional[int]) -> Dict[str, Any]:
        return {
            "id": task.get("id") or "",
            "folder_rel_path": task.get("folder_rel_path") or "",
            "folder_display": task.get("folder_display") or "",
            "source_display_name": task.get("source_display_name") or "",
            "source_saved_name": task.get("source_saved_name") or "",
            "source_rel_path": task.get("source_rel_path") or "",
            "dataset_name": task.get("dataset_name") or "",
            "dataset_id": task.get("dataset_id") or "",
            "markdown_name": task.get("markdown_name") or "",
            "markdown_rel_path": task.get("markdown_rel_path") or "",
            "status": task.get("status") or "",
            "stage": task.get("stage") or "",
            "message": task.get("message") or "",
            "attempt_no": int(task.get("attempt_no") or 0),
            "retry_count": int(task.get("retry_count") or 0),
            "max_retry_count": int(task.get("max_retry_count") or 0),
            "created_at": task.get("created_at") or "",
            "updated_at": task.get("updated_at") or "",
            "started_at": task.get("started_at") or "",
            "finished_at": task.get("finished_at") or "",
            "terminal": bool(task.get("terminal")),
            "doc_id": task.get("doc_id") or "",
            "batch": task.get("batch") or "",
            "indexing_status": task.get("indexing_status") or "",
            "completed_segments": int(task.get("completed_segments") or 0),
            "total_segments": int(task.get("total_segments") or 0),
            "queue_order": queue_order if queue_order is not None else None,
            "last_error": task.get("last_error") or "",
            "result": task.get("result") or "",
        }

    def _prune_locked(self) -> None:
        if len(self._task_order) <= ONDEMAND_QUEUE_HISTORY_LIMIT:
            return

        removable = len(self._task_order) - ONDEMAND_QUEUE_HISTORY_LIMIT
        kept: List[str] = []
        for task_id in self._task_order:
            task = self._tasks.get(task_id)
            if not task:
                continue
            if removable > 0 and task.get("terminal"):
                self._tasks.pop(task_id, None)
                removable -= 1
                continue
            kept.append(task_id)
        self._task_order = kept

    def _ensure_folder_ready_locked(self, folder_rel_path: str) -> None:
        folder = (folder_rel_path or "").strip()
        if not folder:
            return
        if folder == self._running_folder:
            return
        if folder in self._ready_folders:
            return
        if self._folder_queues.get(folder):
            self._ready_folders.append(folder)

    def _build_queue_order_map_locked(self) -> Dict[str, int]:
        order_map: Dict[str, int] = {}
        temp_queues: Dict[str, deque] = {}
        for folder, q in self._folder_queues.items():
            temp_queues[folder] = deque(q)

        temp_ready = deque(self._ready_folders)
        running_task = self._tasks.get(self._running_task_id or "") if self._running_task_id else None
        if running_task:
            running_folder = str(running_task.get("folder_rel_path") or "")
            if temp_queues.get(running_folder):
                temp_ready.append(running_folder)

        order = 1
        while temp_ready:
            folder = temp_ready.popleft()
            q = temp_queues.get(folder)
            if not q:
                continue
            task_id = q.popleft()
            if task_id:
                order_map[task_id] = order
                order += 1
            if q:
                temp_ready.append(folder)
            else:
                temp_queues.pop(folder, None)

        return order_map

    def _queue_order_for_task_locked(self, task_id: str) -> Optional[int]:
        if not task_id:
            return None
        if task_id == self._running_task_id:
            return 0
        return self._build_queue_order_map_locked().get(task_id)

    def _find_task_by_source_signature_locked(self, source_signature: str) -> Optional[Dict[str, Any]]:
        sig = (source_signature or "").strip()
        if not sig:
            return None
        for task_id in reversed(self._task_order):
            task = self._tasks.get(task_id)
            if not task:
                continue
            if str(task.get("source_signature") or "") == sig:
                return task
        return None

    def _remove_task_from_folder_queues_locked(self, task_id: str) -> None:
        if not task_id:
            return
        empty_folders: List[str] = []
        for folder, q in self._folder_queues.items():
            try:
                while task_id in q:
                    q.remove(task_id)
            except ValueError:
                pass
            if not q:
                empty_folders.append(folder)
        for folder in empty_folders:
            self._folder_queues.pop(folder, None)

    def _prune_ready_folders_locked(self) -> None:
        kept = deque()
        seen = set()
        for folder in self._ready_folders:
            if folder in seen:
                continue
            if folder == self._running_folder:
                continue
            if not self._folder_queues.get(folder):
                continue
            kept.append(folder)
            seen.add(folder)
        self._ready_folders = kept

    def _task_is_in_any_queue_locked(self, task_id: str) -> bool:
        if not task_id:
            return False
        if task_id == self._running_task_id:
            return True
        for q in self._folder_queues.values():
            if task_id in q:
                return True
        return False

    def _iter_task_ids_by_source_rel_path_locked(self, source_rel_path: str) -> List[str]:
        rel_key = normalize_name_key(str(source_rel_path or "").replace("\\", "/"))
        if not rel_key:
            return []
        out: List[str] = []
        for task_id in list(self._task_order):
            task = self._tasks.get(task_id)
            if not task:
                continue
            task_rel_key = normalize_name_key(str(task.get("source_rel_path") or "").replace("\\", "/"))
            if task_rel_key == rel_key:
                out.append(task_id)
        return out

    def _iter_handled_signature_keys_by_source_rel_path_locked(self, source_rel_path: str) -> List[str]:
        rel_key = normalize_name_key(str(source_rel_path or "").replace("\\", "/"))
        if not rel_key:
            return []
        out: List[str] = []
        for sig, handled in self._handled_source_signatures.items():
            snapshot = dict((handled or {}).get("snapshot") or {})
            snap_rel_key = normalize_name_key(str(snapshot.get("source_rel_path") or "").replace("\\", "/"))
            if snap_rel_key == rel_key:
                out.append(sig)
        return out

    def _purge_source_rel_path_locked(self, source_rel_path: str, log_status: str = "deleted") -> Dict[str, int]:
        task_ids = self._iter_task_ids_by_source_rel_path_locked(source_rel_path)
        task_id_set = set(task_ids)
        removed_count = 0

        for task_id in task_ids:
            self._remove_task_from_folder_queues_locked(task_id)
            task = self._tasks.pop(task_id, None)
            if not task:
                continue
            doc_key = str(task.get("doc_key") or "")
            if doc_key and self._active_doc_keys.get(doc_key) == task_id:
                self._active_doc_keys.pop(doc_key, None)
            removed_count += 1
            append_ondemand_queue_log("task_deleted", task, status=log_status)

        if task_id_set:
            self._task_order = [task_id for task_id in self._task_order if task_id not in task_id_set]

        handled_keys = self._iter_handled_signature_keys_by_source_rel_path_locked(source_rel_path)
        handled_key_set = set(handled_keys)
        for sig in handled_keys:
            self._handled_source_signatures.pop(sig, None)
        if handled_key_set:
            self._handled_source_order = deque([sig for sig in self._handled_source_order if sig not in handled_key_set])

        self._prune_ready_folders_locked()
        return {
            "removed_count": removed_count,
            "handled_removed_count": len(handled_keys),
        }

    def _revive_failed_task_locked(
        self,
        task: Dict[str, Any],
        dataset: Optional[Dict[str, Any]],
        md_abs_path: str,
        md_rel_path: str,
        md_name: str,
        doc_key: str,
        queue_message: str = "",
    ) -> Dict[str, Any]:
        task_id = str(task.get("id") or "")
        old_doc_key = str(task.get("doc_key") or "")
        if old_doc_key and self._active_doc_keys.get(old_doc_key) == task_id:
            self._active_doc_keys.pop(old_doc_key, None)

        now = now_label()
        task["dataset_id"] = (dataset or {}).get("id") or ""
        task["markdown_abs_path"] = md_abs_path
        task["markdown_rel_path"] = md_rel_path
        task["markdown_name"] = md_name
        task["doc_key"] = doc_key
        task["status"] = "queued"
        task["stage"] = "順番待ち"
        task["message"] = queue_message or "前回エラーのためリトライをリセットして再投入しました。"
        task["attempt_no"] = 0
        task["retry_count"] = 0
        task["started_at"] = ""
        task["finished_at"] = ""
        task["updated_at"] = now
        task["terminal"] = False
        task["doc_id"] = ""
        task["batch"] = ""
        task["indexing_status"] = ""
        task["completed_segments"] = 0
        task["total_segments"] = 0
        task["last_error"] = ""
        task["markdown_written"] = False
        task["result"] = "pending"

        if doc_key:
            self._active_doc_keys[doc_key] = task_id

        folder = str(task.get("folder_rel_path") or "")
        if folder and not self._task_is_in_any_queue_locked(task_id):
            fq = self._folder_queues.setdefault(folder, deque())
            fq.append(task_id)
            self._ensure_folder_ready_locked(folder)

        append_ondemand_queue_log("task_retry_reset", task)
        return task

    def _remember_handled_source_signature_locked(self, source_signature: str, snapshot: Dict[str, Any]) -> None:
        sig = (source_signature or "").strip()
        if not sig:
            return
        self._handled_source_signatures[sig] = {
            "snapshot": dict(snapshot or {}),
            "ts": time.time(),
        }
        self._handled_source_order.append(sig)
        while len(self._handled_source_order) > ONDEMAND_SEEN_SIGNATURE_LIMIT:
            old = self._handled_source_order.popleft()
            if old == sig:
                continue
            self._handled_source_signatures.pop(old, None)

    def _update_task(self, task_id: str, **fields: Any) -> Dict[str, Any]:
        with self._lock:
            task = self._tasks[task_id]
            for key, value in fields.items():
                task[key] = value
            task["updated_at"] = now_label()
            return dict(task)

    def _requeue_task_after_retry(self, task_id: str) -> None:
        with self._cv:
            task = self._tasks[task_id]
            folder = str(task.get("folder_rel_path") or "")
            fq = self._folder_queues.setdefault(folder, deque())
            fq.append(task_id)
            self._ensure_folder_ready_locked(folder)
            self._cv.notify_all()

    def _finish_task(self, task_id: str, status: str, stage: str, message: str, result: str = "") -> None:
        task_snapshot: Dict[str, Any]
        with self._lock:
            task = self._tasks[task_id]
            task["status"] = status
            task["stage"] = stage
            task["message"] = message
            task["terminal"] = True
            task["finished_at"] = now_label()
            task["updated_at"] = task["finished_at"]
            if result:
                task["result"] = result
            doc_key = str(task.get("doc_key") or "")
            if doc_key and self._active_doc_keys.get(doc_key) == task_id:
                self._active_doc_keys.pop(doc_key, None)
            if result in {"completed", "skipped"}:
                self._remember_handled_source_signature_locked(
                    str(task.get("source_signature") or ""),
                    self._public_task_snapshot(task, queue_order=None),
                )
            task_snapshot = dict(task)

        event = {
            "completed": "task_completed",
            "skipped": "task_skipped",
            "error": "task_error",
        }.get(status, "task_finished")
        append_ondemand_queue_log(event, task_snapshot)

    def _cleanup_markdown_if_needed(self, task_id: str) -> None:
        with self._lock:
            task = self._tasks[task_id]
            md_abs_path = str(task.get("markdown_abs_path") or "")
            md_written = bool(task.get("markdown_written"))
            task_snapshot = dict(task)
        if not md_abs_path or not md_written:
            return
        try:
            if os.path.exists(md_abs_path):
                os.remove(md_abs_path)
                append_ondemand_queue_log("markdown_cleanup", task_snapshot, markdown_abs_path=md_abs_path)
        except Exception:
            pass
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id]["markdown_written"] = False

    def _auto_delete_failed_task(self, task_id: str, err: str) -> bool:
        with self._lock:
            task = dict(self._tasks.get(task_id) or {})
        if not task:
            return True

        deleted = delete_ondemand_artifacts(
            folder_rel_path=str(task.get("folder_rel_path") or ""),
            source_abs_path=str(task.get("source_abs_path") or ""),
            source_saved_name=str(task.get("source_saved_name") or ""),
            source_original_name=str(task.get("source_original_name") or ""),
            require_dify_health=False,
            tolerant=True,
        )
        errors = [safe_err(str(x)) for x in (deleted.get("errors") or []) if str(x or "").strip()]
        source_removed = bool(deleted.get("source_deleted")) or not os.path.exists(str(task.get("source_abs_path") or ""))

        if source_removed:
            status = "auto_deleted" if not errors else "auto_deleted_partial"
            with self._cv:
                self._purge_source_rel_path_locked(str(task.get("source_rel_path") or ""), log_status=status)
                self._cv.notify_all()
            return True

        detail = " / ".join(errors[:3]) if errors else "元ファイルを削除できませんでした。"
        with self._lock:
            current = self._tasks.get(task_id)
            if current:
                current["last_error"] = safe_err(str(err))
                current["stage"] = "自動削除失敗"
                current["message"] = f"{safe_err(str(err))} / 自動削除失敗: {detail}"
                current["updated_at"] = now_label()
        append_ondemand_queue_log("task_error", task, status="auto_delete_failed")
        return False

    def _worker_loop(self) -> None:
        while True:
            task_id = ""
            folder = ""
            with self._cv:
                while True:
                    while not self._ready_folders:
                        self._cv.wait(timeout=1.0)
                    folder = self._ready_folders.popleft()
                    q = self._folder_queues.get(folder)
                    if not q:
                        self._folder_queues.pop(folder, None)
                        continue
                    task_id = q.popleft()
                    if not q:
                        self._folder_queues.pop(folder, None)
                    break

                self._running_task_id = task_id
                self._running_folder = folder

            try:
                self._process_one_attempt(task_id)
            except Exception as e:
                err = safe_err(str(e))
                self._cleanup_markdown_if_needed(task_id)
                if not self._auto_delete_failed_task(task_id, err):
                    self._finish_task(
                        task_id,
                        status="error",
                        stage="内部エラー",
                        message=f"{err} / 自動削除に失敗しました。",
                        result="error",
                    )
            finally:
                with self._cv:
                    self._running_task_id = ""
                    self._running_folder = ""
                    if self._folder_queues.get(folder):
                        self._ensure_folder_ready_locked(folder)
                        self._cv.notify_all()

    def _process_one_attempt(self, task_id: str) -> None:
        with self._lock:
            task = self._tasks[task_id]
            task["attempt_no"] = int(task.get("attempt_no") or 0) + 1
            attempt_no = int(task["attempt_no"])
            task["status"] = "running"
            task["stage"] = "処理中"
            task["message"] = f"処理を開始しました（{attempt_no}回目）。"
            if not task.get("started_at"):
                task["started_at"] = now_label()
            task["updated_at"] = now_label()
            task_snapshot = dict(task)

        append_ondemand_queue_log("task_start", task_snapshot)

        try:
            dataset_id = str(task.get("dataset_id") or "")
            source_abs_path = str(task.get("source_abs_path") or "")
            source_display_name = str(task.get("source_display_name") or "")
            markdown_name = str(task.get("markdown_name") or "")
            markdown_abs_path = str(task.get("markdown_abs_path") or "")
            folder_rel_path = str(task.get("folder_rel_path") or "")

            self._update_task(task_id, stage="差分確認", message="同一Markdownがナレッジに存在するか確認しています。")
            if dataset_document_exists_by_name(dataset_id, markdown_name):
                self._finish_task(
                    task_id,
                    status="skipped",
                    stage="差分なし",
                    message="同一Markdownが既にナレッジに存在するため登録をスキップしました。",
                    result="skipped",
                )
                return

            self._update_task(task_id, stage="テキスト抽出", message=f"{source_display_name} からテキストを抽出しています。")
            raw_text, meta = extract_text(source_abs_path, knowledge_style=ONDEMAND_QUEUE_STYLE)
            if not raw_text.strip():
                raise RuntimeError("抽出テキストが空でした。")
            if len(raw_text) > MAX_INPUT_CHARS:
                raw_text = raw_text[:MAX_INPUT_CHARS] + "\n...(truncated)\n"

            self._update_task(task_id, stage="Markdown変換", message="RAG向けMarkdownへ変換しています。")
            md_body = convert_via_dify_chat_messages_secure(
                api_base=API_BASE,
                api_key=API_KEY,
                user=ONDEMAND_QUEUE_USER,
                source_path=folder_rel_path + "/" + source_display_name if folder_rel_path else source_display_name,
                source_meta=meta,
                text=raw_text,
                knowledge_style=ONDEMAND_QUEUE_STYLE,
                chunk_sep=ONDEMAND_QUEUE_CHUNK_SEP,
            )
            md_body = normalize_chunk_sep_lines(md_body, ONDEMAND_QUEUE_CHUNK_SEP)
            md_save = attach_source_metadata(
                md_body,
                source_relpath=str(task.get("source_rel_path") or source_display_name),
                source_abspath=source_abs_path,
                source_meta=meta,
            )

            self._update_task(task_id, stage="Markdown保存", message="Markdownファイルを保存しています。")
            os.makedirs(os.path.dirname(markdown_abs_path), exist_ok=True)
            with open(markdown_abs_path, "w", encoding="utf-8", newline="\n") as f:
                f.write(md_save)
            self._update_task(task_id, markdown_written=True)

            self._update_task(task_id, stage="ナレッジ登録", message="Difyナレッジへ登録しています。")
            reg = register_markdown_to_dify(
                dataset_id=dataset_id,
                doc_name=markdown_name,
                markdown=md_body,
                chunk_sep=ONDEMAND_QUEUE_CHUNK_SEP,
            )
            self._update_task(
                task_id,
                doc_id=reg.get("doc_id") or "",
                batch=reg.get("batch") or "",
                message=(
                    f"受付済み: chunks={reg.get('chunks')} / max_tokens={reg.get('dify_max_tokens')} / search=hybrid_search"
                ),
            )

            final = None
            for prog in iter_indexing_status(dataset_id=dataset_id, batch=reg["batch"], doc_id=reg["doc_id"]):
                completed = int(prog.get("completed_segments") or 0)
                total = int(prog.get("total_segments") or 0)
                status = str(prog.get("indexing_status") or "")
                msg = f"埋め込み中: status={status} / segments={completed}/{total}"
                if prog.get("error"):
                    msg += f" / error={safe_err(str(prog.get('error')))}"
                self._update_task(
                    task_id,
                    stage="埋め込み待ち",
                    indexing_status=status,
                    completed_segments=completed,
                    total_segments=total,
                    message=msg,
                )
                if prog.get("terminal"):
                    final = prog
                    break

            if not final:
                raise RuntimeError("Dify埋め込みの進捗取得に失敗しました。")
            if str(final.get("indexing_status") or "").lower() != "completed":
                raise RuntimeError(
                    f"Dify埋め込み失敗: status={final.get('indexing_status')} error={final.get('error')}"
                )
            if int(final.get("total_segments") or 0) <= 0:
                raise RuntimeError("Dify側で0セグメントのまま完了しました。")

            remember_dataset_document_name(dataset_id, markdown_name)
            self._finish_task(
                task_id,
                status="completed",
                stage="完了",
                message="Markdown保存とナレッジ登録が完了しました。",
                result="completed",
            )
        except Exception as e:
            err = safe_err(str(e))
            with self._lock:
                task = self._tasks[task_id]
                retry_count = int(task.get("retry_count") or 0)
                max_retry = int(task.get("max_retry_count") or 0)
                task["last_error"] = err

            if retry_count < max_retry:
                retry_count += 1
                with self._lock:
                    task = self._tasks[task_id]
                    task["retry_count"] = retry_count
                    task["status"] = "queued"
                    task["stage"] = "リトライ待ち"
                    task["message"] = f"{err} / リトライ {retry_count}/{max_retry} を待機しています。"
                    task["updated_at"] = now_label()
                    task["terminal"] = False
                    task["result"] = "pending"
                    task_snapshot = dict(task)
                append_ondemand_queue_log("task_retry_wait", task_snapshot, error=err)
                self._requeue_task_after_retry(task_id)
                return

            self._cleanup_markdown_if_needed(task_id)
            if self._auto_delete_failed_task(task_id, err):
                return
            with self._lock:
                current = self._tasks.get(task_id)
                finish_stage = str((current or {}).get("stage") or "エラー終了")
                finish_message = str((current or {}).get("message") or f"{err} / リトライ上限に達したため中止しました。")
            self._finish_task(
                task_id,
                status="error",
                stage=finish_stage,
                message=finish_message,
                result="error",
            )


    def get_error_task_for_action(self, task_id: str) -> Dict[str, Any]:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return {"ok": False, "error": "タスクが見つかりません。"}
            if str(task.get("status") or "") != "error" or not bool(task.get("terminal")):
                return {"ok": False, "error": "エラー状態のタスクのみ操作できます。"}
            if task_id == self._running_task_id:
                return {"ok": False, "error": "処理中のため操作できません。"}
            return {"ok": True, "task": dict(task)}

    def remove_error_task(self, task_id: str) -> None:
        with self._cv:
            task = self._tasks.get(task_id)
            if not task:
                return
            self._remove_task_from_folder_queues_locked(task_id)
            self._tasks.pop(task_id, None)
            self._task_order = [tid for tid in self._task_order if tid != task_id]
            doc_key = str(task.get("doc_key") or "")
            if doc_key and self._active_doc_keys.get(doc_key) == task_id:
                self._active_doc_keys.pop(doc_key, None)
            append_ondemand_queue_log("task_deleted", task, status="deleted")
            self._prune_ready_folders_locked()
            self._cv.notify_all()

    def retry_error_task(self, task_id: str, dataset_id_override: str = "") -> Dict[str, Any]:
        with self._cv:
            task = self._tasks.get(task_id)
            if not task:
                return {"ok": False, "error": "タスクが見つかりません。"}
            if str(task.get("status") or "") != "error" or not bool(task.get("terminal")):
                return {"ok": False, "error": "エラー状態のタスクのみ操作できます。"}
            if task_id == self._running_task_id:
                return {"ok": False, "error": "処理中のため操作できません。"}

            dataset_id = dataset_id_override or str(task.get("dataset_id") or "")
            dataset_name = str(task.get("dataset_name") or "")
            md_abs_path = str(task.get("markdown_abs_path") or "")
            md_rel_path = str(task.get("markdown_rel_path") or "")
            md_name = str(task.get("markdown_name") or "")

            if not dataset_id:
                return {"ok": False, "error": "ナレッジIDが不明なため再試行できません。"}
            if not md_abs_path:
                return {"ok": False, "error": "Markdownパスが不明なため再試行できません。"}

            dataset = {"id": dataset_id}
            doc_key = build_ondemand_doc_key(dataset_id, dataset_name, md_name)

            revived = self._revive_failed_task_locked(
                task,
                dataset=dataset,
                md_abs_path=md_abs_path,
                md_rel_path=md_rel_path,
                md_name=md_name,
                doc_key=doc_key,
                queue_message="手動再試行によりキューの最後尾に追加しました。",
            )
            self._cv.notify_all()
            return {
                "ok": True,
                "task": self._public_task_snapshot(revived, self._queue_order_for_task_locked(task_id)),
            }


class OnDemandFolderMonitor:
    def __init__(self, queue_manager: OnDemandQueueManager):
        self._queue = queue_manager
        self._lock = threading.RLock()
        self._started = False
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._last_scan_started_at = ""
        self._last_scan_finished_at = ""
        self._last_scan_error = ""
        self._last_stats: Dict[str, Any] = {
            "folders": 0,
            "files": 0,
            "enqueued": 0,
            "known": 0,
            "doc_exists": 0,
            "dataset_missing": 0,
            "not_target": 0,
        }

    def start(self) -> None:
        if not ONDEMAND_MONITOR_ENABLED:
            return
        with self._lock:
            if self._started:
                return
            self._thread = threading.Thread(target=self._loop, name="ondemand-folder-monitor", daemon=True)
            self._thread.start()
            self._started = True

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": bool(ONDEMAND_MONITOR_ENABLED),
                "running": bool(self._running),
                "interval_sec": ONDEMAND_MONITOR_INTERVAL_SEC,
                "last_scan_started_at": self._last_scan_started_at,
                "last_scan_finished_at": self._last_scan_finished_at,
                "last_scan_error": self._last_scan_error,
                "last_stats": dict(self._last_stats),
            }

    def _set_scan_state(self, **fields: Any) -> None:
        with self._lock:
            for key, value in fields.items():
                setattr(self, key, value)

    def _loop(self) -> None:
        while True:
            started_at = now_label()
            self._set_scan_state(_running=True, _last_scan_started_at=started_at, _last_scan_error="")
            stats = {
                "folders": 0,
                "files": 0,
                "enqueued": 0,
                "known": 0,
                "doc_exists": 0,
                "dataset_missing": 0,
                "not_target": 0,
            }
            last_error = ""

            try:
                self._scan_once(stats)
            except Exception as e:
                last_error = safe_err(str(e))

            finished_at = now_label()
            with self._lock:
                self._running = False
                self._last_scan_finished_at = finished_at
                self._last_scan_error = last_error
                self._last_stats = dict(stats)

            time.sleep(ONDEMAND_MONITOR_INTERVAL_SEC)

    def _scan_once(self, stats: Dict[str, int]) -> None:
        if not API_BASE or not API_KEY or not DATASET_API_BASE or not DATASET_API_KEY:
            return

        datasets = get_datasets_cached(force=False)
        dataset_map = {normalize_name_key((it or {}).get("name")): dict(it) for it in (datasets or []) if (it or {}).get("name")}

        for folder_abs_path, folder_rel_path in iter_ondemand_watch_folders(EXPLORER_ROOT):
            stats["folders"] += 1

            if not is_ondemand_source_folder_rel(folder_rel_path):
                stats["not_target"] += 1
                continue

            dataset_name = build_ondemand_dataset_name(folder_rel_path)
            dataset = dataset_map.get(normalize_name_key(dataset_name)) if dataset_name else None

            files = list_ondemand_source_files(folder_abs_path, EXPLORER_ROOT)
            for info in files:
                stats["files"] += 1
                source_signature = str(info.get("source_signature") or "")
                if source_signature and self._queue.get_task_snapshot_by_source_signature(source_signature):
                    stats["known"] += 1
                    continue

                if not dataset:
                    stats["dataset_missing"] += 1
                    continue

                md_name = ""
                try:
                    _, _, md_name = build_ondemand_markdown_path(folder_rel_path, str(info.get("source_original_name") or ""))
                except Exception:
                    continue

                if dataset_document_exists_by_name(str(dataset.get("id") or ""), md_name):
                    self._queue.remember_handled_source_signature(
                        source_signature=source_signature,
                        status="skipped",
                        stage="差分なし",
                        message="同一Markdownが既にナレッジに存在するため監視対象から除外しました。",
                        result="skipped",
                    )
                    stats["doc_exists"] += 1
                    continue

                task = self._queue.enqueue_saved_file(
                    folder_rel_path=folder_rel_path,
                    folder_abs_path=folder_abs_path,
                    source_abs_path=str(info.get("source_abs_path") or ""),
                    source_rel_path=str(info.get("source_rel_path") or ""),
                    source_saved_name=str(info.get("source_saved_name") or ""),
                    source_original_name=str(info.get("source_original_name") or ""),
                    source_signature=source_signature,
                    dataset_hint=dataset,
                    queue_message="フォルダ監視で検出し、順番待ちキューへ追加しました。",
                )
                if task and str(task.get("status") or "") == "queued":
                    stats["enqueued"] += 1
                else:
                    stats["known"] += 1


__all__ = ["OnDemandQueueManager", "OnDemandFolderMonitor"]
