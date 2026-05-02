def _confirm_run(container, key: str, label: str = "輸入 RUN 以確認"):
    token = container.text_input(label, value="", key=f"admin_confirm_{str(key)}")
    return str(token or "").strip().upper() == "RUN"
