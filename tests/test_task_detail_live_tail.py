from pathlib import Path
import unittest


class TaskDetailLiveTailTemplateTests(unittest.TestCase):
    def test_selected_task_page_polls_only_while_visible(self) -> None:
        template = (
            Path(__file__).resolve().parents[1] / "templates" / "task_detail.html"
        ).read_text(encoding="utf-8")

        self.assertIn('fetch(url, { cache: "no-store" })', template)
        self.assertIn('fetch("/api/tasks/" + taskId, { cache: "no-store" })', template)
        self.assertIn("setInterval(refreshLiveLogs, 5000)", template)
        self.assertIn('document.addEventListener("visibilitychange"', template)
        self.assertIn("if (refreshing || document.hidden) return", template)
        self.assertIn("if (document.hidden)", template)
        self.assertIn("stopPolling()", template)

    def test_live_refresh_preserves_last_good_log_and_updates_task_state(self) -> None:
        template = (
            Path(__file__).resolve().parents[1] / "templates" / "task_detail.html"
        ).read_text(encoding="utf-8")

        self.assertIn("if (!loaded[elementId])", template)
        self.assertIn("nearBottom", template)
        self.assertIn('id="task-live-status"', template)
        self.assertIn('id="task-live-exit-code"', template)
        self.assertIn('id="task-live-failure"', template)
        self.assertIn("terminalStates.has(state)", template)


if __name__ == "__main__":
    unittest.main()
