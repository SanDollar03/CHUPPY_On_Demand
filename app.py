# app.py
# -*- coding: utf-8 -*-

from flask import Flask, render_template, request, jsonify

from ondemand_core import *
from ondemand_queue import OnDemandQueueManager, OnDemandFolderMonitor


ONDEMAND_QUEUE = OnDemandQueueManager()
ONDEMAND_MONITOR = OnDemandFolderMonitor(ONDEMAND_QUEUE)


def create_app():
    ensure_queue_log_file()

    app = Flask(__name__)
    app.config["JSON_AS_ASCII"] = False

    ONDEMAND_QUEUE.start()
    ONDEMAND_MONITOR.start()

    @app.get("/")
    @app.get("/ondemand")
    def ondemand_page():
        return render_template(
            "ondemand.html",
            title=APP_TITLE + " (ON DEMAND)",
            model_label=HEADER_MODEL_LABEL,
            explorer_root=EXPLORER_ROOT,
            explorer_max_depth=EXPLORER_MAX_DEPTH,
            upload_allowed_depth=UPLOAD_ALLOWED_DEPTH,
            allowed_extensions=get_allowed_extensions(),
            allowed_extensions_text=get_allowed_extensions_text(),
            max_upload_file_size_bytes=get_upload_max_file_size_bytes(),
        )

    @app.get("/api/datasets")
    def api_datasets():
        # Difyナレッジ登録は行わないため、常に空のリストを返す
        return jsonify({"ok": True, "items": [], "prefix": DATASET_NAME_PREFIX})

    @app.get("/api/explorer/root")
    def api_explorer_root():
        try:
            stats_cache: Dict[str, Dict[str, int]] = {}
            root_info = build_dir_info(EXPLORER_ROOT, EXPLORER_ROOT, stats_cache)
            root_info["depth"] = 0
            root_info["can_upload"] = False
            root_info["has_children"] = dir_has_visible_child_dirs(EXPLORER_ROOT, EXPLORER_ROOT)
            return jsonify({
                "ok": True,
                "root": root_info,
                "max_depth": EXPLORER_MAX_DEPTH,
                "upload_allowed_depth": UPLOAD_ALLOWED_DEPTH,
                "allowed_extensions": get_allowed_extensions(),
            })
        except Exception as e:
            return jsonify({"ok": False, "error": safe_err(str(e))}), 500

    @app.get("/api/explorer/list")
    def api_explorer_list():
        rel_path = (request.args.get("path") or "").strip()
        force_status = str(request.args.get("force_status") or "").strip().lower() in {"1", "true", "yes", "on"}
        try:
            abs_dir = resolve_explorer_path(EXPLORER_ROOT, rel_path)
            if not os.path.isdir(abs_dir):
                return jsonify({"ok": False, "error": "対象フォルダが存在しません。"}), 404
            dirs, files = list_explorer_dir(abs_dir, EXPLORER_ROOT)
            stats_cache: Dict[str, Dict[str, int]] = {}
            current = build_dir_info(abs_dir, EXPLORER_ROOT, stats_cache)
            if current.get("can_upload") and files:
                files = enrich_explorer_files_with_registration_status(
                    folder_rel_path=str(current.get("path") or ""),
                    files=files,
                    queue_manager=ONDEMAND_QUEUE,
                    force=force_status,
                )
            delete_available, delete_reason = get_dify_delete_availability(force=False)
            current["delete_available"] = bool(delete_available)
            current["delete_reason"] = delete_reason or ""
            return jsonify({
                "ok": True,
                "current": current,
                "dirs": dirs,
                "files": files,
                "max_depth": EXPLORER_MAX_DEPTH,
                "upload_allowed_depth": UPLOAD_ALLOWED_DEPTH,
                "allowed_extensions": get_allowed_extensions(),
            })
        except Exception as e:
            return jsonify({"ok": False, "error": safe_err(str(e))}), 400

    @app.post("/api/explorer/upload")
    def api_explorer_upload():
        rel_path = (request.form.get("path") or "").strip()
        try:
            target_dir = resolve_explorer_path(EXPLORER_ROOT, rel_path)
        except Exception as e:
            return jsonify({"ok": False, "error": safe_err(str(e))}), 400

        if not os.path.isdir(target_dir):
            return jsonify({"ok": False, "error": "保存先フォルダが存在しません。"}), 404

        depth = path_depth_from_root(target_dir, EXPLORER_ROOT)
        if depth != UPLOAD_ALLOWED_DEPTH:
            return jsonify({
                "ok": False,
                "error": f"ファイル追加が許可されているのは {UPLOAD_ALLOWED_DEPTH} 階層目のフォルダのみです。"
            }), 400

        files = request.files.getlist("files")
        if not files:
            return jsonify({"ok": False, "error": "アップロードするファイルがありません。"}), 400
        if len(files) > UPLOAD_MAX_FILES:
            return jsonify({"ok": False, "error": f"一度にアップロードできるのは最大 {UPLOAD_MAX_FILES} 件です。"}), 400

        saved = []
        skipped = []
        errors = []
        queue_items = []
        queue_errors = []
        folder_rel_path = make_rel_from_root(target_dir, EXPLORER_ROOT)

        for f in files:
            try:
                original_name_raw = (f.filename or "").strip()
                if not original_name_raw:
                    skipped.append({"name": "", "reason": "ファイル名が空です。"})
                    continue

                original_name = sanitize_upload_filename(original_name_raw)
                if not original_name:
                    skipped.append({"name": original_name_raw, "reason": "使用できないファイル名です。"})
                    continue

                ext = os.path.splitext(original_name)[1].lower()
                if not is_allowed_extension(ext):
                    skipped.append({
                        "name": original_name,
                        "reason": f"非対応拡張子です: {ext or '(拡張子なし)'}",
                    })
                    continue

                file_size_bytes = get_filestorage_size_bytes(f)
                if file_size_bytes > UPLOAD_MAX_FILE_SIZE_BYTES:
                    skipped.append({
                        "name": original_name,
                        "reason": f"100MBを超えるファイルはアップロードできません ({format_size_bytes(file_size_bytes)})",
                    })
                    continue

                stored_name = add_upload_timestamp_prefix(original_name)
                save_path = build_unique_upload_path(target_dir, stored_name)
                saved_name = os.path.basename(save_path)
                f.save(save_path)

                saved_item = {
                    "name": saved_name,
                    "original_name": original_name,
                    "rel_path": make_rel_from_root(save_path, EXPLORER_ROOT),
                    "size_bytes": os.path.getsize(save_path) if os.path.exists(save_path) else 0,
                }
                saved.append(saved_item)

                try:
                    task = ONDEMAND_QUEUE.enqueue_saved_file(
                        folder_rel_path=folder_rel_path,
                        folder_abs_path=target_dir,
                        source_abs_path=save_path,
                        source_rel_path=saved_item["rel_path"],
                        source_saved_name=saved_name,
                        source_original_name=original_name,
                    )
                    if task:
                        queue_items.append(task)
                except Exception as qerr:
                    queue_errors.append({"name": original_name, "error": safe_err(str(qerr))})
            except Exception as e:
                errors.append({"name": f.filename or "", "error": safe_err(str(e))})

        return jsonify({
            "ok": True,
            "target": build_dir_info(target_dir, EXPLORER_ROOT, {}),
            "saved": saved,
            "skipped": skipped,
            "errors": errors,
            "queue_items": queue_items,
            "queue_errors": queue_errors,
            "max_upload_file_size_bytes": get_upload_max_file_size_bytes(),
        })

    @app.post("/api/explorer/delete")
    def api_explorer_delete():
        payload = request.get_json(silent=True) or {}
        rel_path = str(payload.get("path") or "").strip()
        if not rel_path:
            return jsonify({"ok": False, "error": "削除対象ファイルのパスが指定されていません。"}), 400

        try:
            source_abs_path = resolve_explorer_path(EXPLORER_ROOT, rel_path)
        except Exception as e:
            return jsonify({"ok": False, "error": safe_err(str(e))}), 400

        if not os.path.isfile(source_abs_path):
            return jsonify({"ok": False, "error": "削除対象ファイルが存在しません。"}), 404

        source_rel_path = make_rel_from_root(source_abs_path, EXPLORER_ROOT)
        source_saved_name = os.path.basename(source_abs_path)
        source_ext = os.path.splitext(source_saved_name)[1].lower()
        if not is_allowed_extension(source_ext):
            return jsonify({"ok": False, "error": f"非対応拡張子のため削除できません: {source_ext or '(拡張子なし)'}"}), 400

        folder_abs_path = os.path.dirname(source_abs_path)
        folder_rel_path = make_rel_from_root(folder_abs_path, EXPLORER_ROOT)
        if path_depth_from_root(folder_abs_path, EXPLORER_ROOT) != UPLOAD_ALLOWED_DEPTH:
            return jsonify({"ok": False, "error": f"削除できるのは Lv{UPLOAD_ALLOWED_DEPTH} フォルダ内のファイルのみです。"}), 400

        delete_available, delete_reason = get_dify_delete_availability(force=True)
        if not delete_available:
            return jsonify({"ok": False, "error": delete_reason or "Difyに通信できないため削除できません。"}), 503

        pause_info = ONDEMAND_QUEUE.pause_delete_for_source(source_rel_path)
        if not pause_info.get("ok"):
            return jsonify({"ok": False, "error": pause_info.get("error") or "このファイルは現在削除できません。"}), 409

        try:
            deleted = delete_ondemand_artifacts(
                folder_rel_path=folder_rel_path,
                source_abs_path=source_abs_path,
                source_saved_name=source_saved_name,
                source_original_name="",
                require_dify_health=False,
                tolerant=False,
            )
            purge_info = ONDEMAND_QUEUE.finalize_paused_delete(
                pause_info,
                message="元ファイル削除によりキューから除外しました。",
            )
            deleted["source_rel_path"] = source_rel_path
            deleted["queue_paused_count"] = int(pause_info.get("paused_count") or 0)
            deleted["queue_removed_count"] = int((purge_info or {}).get("removed_count") or 0)
            deleted["queue_handled_removed_count"] = int((purge_info or {}).get("handled_removed_count") or 0)
            return jsonify({
                "ok": True,
                "deleted": deleted,
            })
        except Exception as e:
            ONDEMAND_QUEUE.restore_paused_delete(
                pause_info,
                message="削除に失敗したためキューを戻しました。",
            )
            return jsonify({"ok": False, "error": safe_err(str(e))}), 500

    @app.post("/api/ondemand/queue/<task_id>/delete")
    def api_ondemand_queue_task_delete(task_id):
        result = ONDEMAND_QUEUE.get_error_task_for_action(task_id)
        if not result.get("ok"):
            return jsonify({"ok": False, "error": result.get("error")}), 400
        task = result["task"]
        try:
            deleted = delete_ondemand_artifacts(
                folder_rel_path=str(task.get("folder_rel_path") or ""),
                source_abs_path=str(task.get("source_abs_path") or ""),
                source_saved_name=str(task.get("source_saved_name") or ""),
                source_original_name=str(task.get("source_original_name") or ""),
                require_dify_health=False,
                tolerant=True,
            )
        except Exception as e:
            return jsonify({"ok": False, "error": safe_err(str(e))}), 500
        ONDEMAND_QUEUE.remove_error_task(task_id)
        return jsonify({"ok": True, "deleted": deleted})

    @app.post("/api/ondemand/queue/<task_id>/retry")
    def api_ondemand_queue_task_retry(task_id):
        result = ONDEMAND_QUEUE.get_error_task_for_action(task_id)
        if not result.get("ok"):
            return jsonify({"ok": False, "error": result.get("error")}), 400
        task = result["task"]

        if not str(task.get("markdown_abs_path") or ""):
            return jsonify({"ok": False, "error": "Markdownパスが不明なため再試行できません。"}), 400

        try:
            cleanup = cleanup_markdown_only(
                markdown_abs_path=str(task.get("markdown_abs_path") or ""),
            )
        except Exception as e:
            return jsonify({"ok": False, "error": safe_err(str(e))}), 500

        retry_result = ONDEMAND_QUEUE.retry_error_task(task_id)
        if not retry_result.get("ok"):
            return jsonify({"ok": False, "error": retry_result.get("error")}), 400
        return jsonify({"ok": True, "task": retry_result.get("task"), "cleanup": cleanup})

    @app.get("/api/ondemand/queue")
    def api_ondemand_queue():
        try:
            limit = int(request.args.get("limit") or "200")
        except Exception:
            limit = 200
        limit = max(20, min(500, limit))
        include_completed = str(request.args.get("include_completed") or "").strip().lower() in {"1", "true", "yes", "on"}
        try:
            snap = ONDEMAND_QUEUE.get_snapshot(limit=limit, hide_completed=not include_completed)
            monitor = ONDEMAND_MONITOR.get_status()
            return jsonify({"ok": True, **snap, "monitor": monitor})
        except Exception as e:
            return jsonify({"ok": False, "error": safe_err(str(e))}), 500

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5222, debug=False, threaded=True)