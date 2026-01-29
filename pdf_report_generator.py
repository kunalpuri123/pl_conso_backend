from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

def generate_pdf_from_ai_report(ai_report: dict, output_path: str):
    doc = SimpleDocTemplate(output_path, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []

    def add(title, value):
        story.append(Paragraph(f"<b>{title}</b>: {value}", styles["Normal"]))
        story.append(Spacer(1, 8))

    add("Summary", ai_report.get("summary"))
    add("Verdict", ai_report.get("verdict"))
    add("Accuracy", f"{ai_report.get('accuracy')}%")
    add("Total Tested", ai_report.get("total_tested"))
    add("Passed", ai_report.get("passed"))
    add("Failed", ai_report.get("failed"))

    story.append(Spacer(1, 12))
    story.append(Paragraph("<b>Top Issues:</b>", styles["Heading3"]))

    for issue in ai_report.get("top_issues", []):
        story.append(Paragraph(f"- {issue}", styles["Normal"]))

    story.append(Spacer(1, 12))
    story.append(Paragraph("<b>Errors:</b>", styles["Heading3"]))

    for err in ai_report.get("errors", []):
        story.append(Paragraph(f"- {err}", styles["Normal"]))

    doc.build(story)
