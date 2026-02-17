"""PDF Report Generator for Institutional Decision Tracking.

매수/매도 의사결정을 PDF로 생성하는 모듈:
- Watchlist Final 30 선정 이유
- Minervini 통과 종목 이유
- Exit Analysis (매수 당시 vs 현재 비교)
"""
from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

logger = logging.getLogger(__name__)

# 한글 폰트 설정 시도 (없으면 기본 폰트 사용)
try:
    # NanumGothic 폰트 경로 (시스템에 따라 다를 수 있음)
    font_paths = [
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
        "/System/Library/Fonts/AppleSDGothicNeo.ttc",  # macOS
    ]
    
    for font_path in font_paths:
        if os.path.exists(font_path):
            pdfmetrics.registerFont(TTFont("NanumGothic", font_path))
            KOREAN_FONT = "NanumGothic"
            break
    else:
        KOREAN_FONT = "Helvetica"
        logger.warning("[PDF] Korean font not found, using Helvetica")
except Exception as e:
    KOREAN_FONT = "Helvetica"
    logger.warning("[PDF] Font registration failed: %s, using Helvetica", e)


def _create_styles():
    """PDF 스타일 생성."""
    styles = getSampleStyleSheet()
    
    # 제목
    styles.add(ParagraphStyle(
        name='KoreanTitle',
        parent=styles['Heading1'],
        fontName=KOREAN_FONT,
        fontSize=18,
        spaceAfter=12,
        textColor=colors.HexColor("#1a1a1a"),
    ))
    
    # 부제목
    styles.add(ParagraphStyle(
        name='KoreanHeading2',
        parent=styles['Heading2'],
        fontName=KOREAN_FONT,
        fontSize=14,
        spaceAfter=10,
        textColor=colors.HexColor("#333333"),
    ))
    
    # 본문
    styles.add(ParagraphStyle(
        name='KoreanBody',
        parent=styles['BodyText'],
        fontName=KOREAN_FONT,
        fontSize=10,
        leading=14,
    ))
    
    return styles


def _normalize_reasons_for_pdf(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        if all(isinstance(x, str) for x in value):
            return {"bullets": list(value)}
        if all(isinstance(x, dict) for x in value):
            merged: Dict[str, Any] = {}
            can_merge = True
            for entry in value:
                for key, item_value in entry.items():
                    if key in merged:
                        can_merge = False
                        break
                    merged[key] = item_value
                if not can_merge:
                    break
            return merged if can_merge else {"items": list(value)}
        return {"raw": str(value)}
    return {"raw": str(value)}


def generate_watchlist_pdf(
    *,
    final30: Optional[List[Dict[str, Any]]] = None,
    pool120: Optional[List[Dict[str, Any]]] = None,
    top50: Optional[List[Dict[str, Any]]] = None,
    reject_summary: Optional[Dict[str, Any]] = None,
    weights: Optional[Dict[str, Any]] = None,
    as_of: date,
    output_dir: Optional[Path] = None,
) -> Path:
    """
    Final 30 선정 이유 PDF 생성.
    
    Args:
        final30: Final 30 종목 리스트
        pool120: Pool 120 종목 리스트
        top50: Top 50 종목 리스트
        reject_summary: 탈락 사유 집계
        weights: 가중치 정보
        as_of: 기준일
        output_dir: 출력 디렉토리 (기본값: runtime/watchlist/YYYY-MM-DD/)
    
    Returns:
        생성된 PDF 파일 경로
    """
    if output_dir is None:
        output_dir = Path("runtime/watchlist") / as_of.strftime("%Y-%m-%d")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "report.pdf"

    final30 = final30 or []
    top50 = top50 or []
    pool120 = pool120 or []
    reject_summary = reject_summary or {}
    weights = weights or {"tech_weight": 0.7, "flow_weight": 0.3}
    
    logger.info("[REPORT][WATCHLIST][PDF] generating as_of=%s output=%s", as_of, output_path)
    
    # PDF 문서 생성
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        rightMargin=30,
        leftMargin=30,
        topMargin=30,
        bottomMargin=30,
    )
    
    story = []
    styles = _create_styles()
    
    # 제목
    story.append(Paragraph("Unified Watchlist Pipeline Report", styles['KoreanTitle']))
    story.append(Paragraph(f"Date: {as_of.strftime('%Y-%m-%d')}", styles['KoreanBody']))
    story.append(Spacer(1, 0.3 * inch))
    
    # 요약
    story.append(Paragraph("Selection Summary", styles['KoreanHeading2']))
    summary_text = (
        f"Total candidates analyzed: {len(pool120)}<br/>"
        f"Top 50 filtered: {len(top50)}<br/>"
        f"Final selection: {len(final30)}<br/>"
        f"Selection criteria: FinalScore = TechScore({weights.get('tech_weight', 0.7):.2f}) "
        f"+ FlowScore({weights.get('flow_weight', 0.3):.2f})"
    )
    story.append(Paragraph(summary_text, styles['KoreanBody']))
    story.append(Spacer(1, 0.3 * inch))
    
    def _append_table(title: str, rows: List[Dict[str, Any]], max_rows: int):
        story.append(Paragraph(title, styles['KoreanHeading2']))
        table_data = [["Rank", "Code", "Tech", "Flow", "Final", "Reject Reasons"]]
        if not rows:
            table_data.append(["-", "-", "-", "-", "-", "No rows"])
        else:
            for item in rows[:max_rows]:
                reject_reasons = item.get("reject_reasons") or (item.get("meta") or {}).get("reject_reasons") or []
                reasons_dict = _normalize_reasons_for_pdf(item.get("reasons"))
                failed_reasons = reasons_dict.get("failed")
                if isinstance(failed_reasons, list) and failed_reasons:
                    reject_txt = ", ".join([str(x) for x in failed_reasons[:3]])
                elif reject_reasons:
                    reject_txt = ", ".join([str(x) for x in reject_reasons[:3]])
                elif isinstance(reasons_dict.get("bullets"), list) and reasons_dict.get("bullets"):
                    reject_txt = ", ".join([str(x) for x in reasons_dict.get("bullets", [])[:3]])
                else:
                    reject_txt = "-"
                table_data.append(
                    [
                        str(item.get("rank", "-")),
                        str(item.get("code", "-")),
                        f"{float(item.get('tech_score', 0) or 0):.1f}",
                        f"{float(item.get('flow_score', 0) or 0):.3f}",
                        f"{float(item.get('final_score', item.get('score', 0)) or 0):.1f}",
                        reject_txt,
                    ]
                )

        table = Table(table_data, colWidths=[0.6 * inch, 0.9 * inch, 0.8 * inch, 0.8 * inch, 0.8 * inch, 2.6 * inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        story.append(table)
        story.append(Spacer(1, 0.25 * inch))
    
    _append_table("Pool 120", pool120, 20)
    _append_table("Top 50", top50, 20)
    _append_table("Final 30", final30, 30)

    story.append(Paragraph("Reject Summary", styles['KoreanHeading2']))
    if reject_summary:
        reject_lines = "<br/>".join([f"- {k}: {v}" for k, v in sorted(reject_summary.items(), key=lambda x: x[1], reverse=True)])
    else:
        reject_lines = "- no reject reasons"
    story.append(Paragraph(reject_lines, styles['KoreanBody']))
    story.append(Spacer(1, 0.3 * inch))
    
    # Top 5 상세 분석
    story.append(PageBreak())
    story.append(Paragraph("Top 5 Detailed Analysis", styles['KoreanHeading2']))
    
    for i, item in enumerate(final30[:5], 1):
        reasons = _normalize_reasons_for_pdf(item.get("reasons"))
        notes = reasons.get("notes") if isinstance(reasons.get("notes"), dict) else {}
        scores = item.get("scores") if isinstance(item.get("scores"), dict) else {}
        reject_reasons = item.get("reject_reasons") or (item.get("meta") or {}).get("reject_reasons") or []
        
        detail_text = f"""
        <b>#{i}: {item.get('code')} - {item.get('name', 'N/A')}</b><br/>
        Final Score: {item.get('final_score', 0):.2f} (Tech: {item.get('tech_score', 0):.1f}, Flow: {item.get('flow_score', 0):.2f})<br/>
        <br/>
        <b>Selection Reasons:</b><br/>
        - Trend Template: {'Yes' if scores.get('trend_template', 0) else 'No'}<br/>
        - RS Percentile: {item.get('rs_pctile', scores.get('rs_pctile', notes.get('rs_pctile', 0))): .1f}<br/>
        - VCP Score: {item.get('vcp_score', scores.get('vcp_score', notes.get('vcp_score', 0))):.1f}<br/>
        - Pullback: {item.get('pullback_pct', notes.get('pullback_pct', 0)): .2%}<br/>
        - Foreign 20D Flow: {item.get('foreign_20_ratio', notes.get('foreign_net_20d', 0)): .3f}<br/>
        - Institutional 20D Flow: {item.get('inst_20_ratio', notes.get('inst_net_20d', 0)): .3f}<br/>
        - Dollar Volume Rank: {scores.get('liquidity_rank', reasons.get('dollar_vol_rank', 'N/A'))}<br/>
        - Reject Reasons: {', '.join(reject_reasons) if reject_reasons else '-'}<br/>
        """
        
        story.append(Paragraph(detail_text, styles['KoreanBody']))
        story.append(Spacer(1, 0.2 * inch))
    
    # PDF 빌드
    doc.build(story)
    
    logger.info("[PDF] wrote %s", output_path)
    return output_path


def generate_minervini_pdf(
    *,
    passed_list: List[Dict[str, Any]],
    as_of: date,
    output_dir: Optional[Path] = None,
) -> Path:
    """
    Minervini 통과 종목 PDF 생성.
    
    Args:
        passed_list: 통과 종목 리스트 (각 항목에 code, name, rs_percentile, vcp_score, trend_ok, score, reasons 포함)
        as_of: 기준일
        output_dir: 출력 디렉토리 (기본값: runtime/reports/minervini/YYYY-MM-DD/)
    
    Returns:
        생성된 PDF 파일 경로
    """
    if output_dir is None:
        output_dir = Path("runtime/reports/minervini") / as_of.strftime("%Y-%m-%d")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "minervini_report.pdf"
    
    logger.info("[REPORT][MINERVINI][PDF] generating as_of=%s output=%s", as_of, output_path)
    
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        rightMargin=30,
        leftMargin=30,
        topMargin=30,
        bottomMargin=30,
    )
    
    story = []
    styles = _create_styles()
    
    # 제목
    story.append(Paragraph(f"Minervini Filter Pass Report", styles['KoreanTitle']))
    story.append(Paragraph(f"Date: {as_of.strftime('%Y-%m-%d')}", styles['KoreanBody']))
    story.append(Spacer(1, 0.3 * inch))
    
    # 요약
    story.append(Paragraph("Filter Summary", styles['KoreanHeading2']))
    summary_text = f"""
    Total passed: {len(passed_list)}<br/>
    Criteria: RS Percentile, VCP Pattern, Trend Template, Volume Dry-Up
    """
    story.append(Paragraph(summary_text, styles['KoreanBody']))
    story.append(Spacer(1, 0.3 * inch))
    
    # 통과 종목 테이블
    story.append(Paragraph("Passed Stocks", styles['KoreanHeading2']))
    
    table_data = [
        ["Code", "Name", "RS %ile", "VCP", "Trend", "Score", "Key Reasons"]
    ]
    
    for item in passed_list:
        reasons = item.get("reasons", {})
        key_reasons_list = []
        
        if reasons.get("rs_high"):
            key_reasons_list.append(f"RS{int(item.get('rs_percentile', 0))}")
        if reasons.get("vcp_detected"):
            key_reasons_list.append(f"VCP{item.get('vcp_score', 0):.1f}")
        if reasons.get("volume_contraction"):
            key_reasons_list.append("Vol↓")
        
        key_reasons_str = ", ".join(key_reasons_list[:3]) if key_reasons_list else "-"
        
        table_data.append([
            item.get("code", "-"),
            item.get("name", "-")[:10],
            f"{item.get('rs_percentile', 0):.1f}",
            "✓" if item.get("vcp_score", 0) > 0 else "",
            "✓" if item.get("trend_ok") else "",
            f"{item.get('score', 0):.1f}",
            key_reasons_str,
        ])
    
    table = Table(table_data, colWidths=[0.8*inch, 1.2*inch, 0.8*inch, 0.5*inch, 0.6*inch, 0.7*inch, 2.2*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    
    story.append(table)
    story.append(Spacer(1, 0.3 * inch))
    
    # 상위 종목 상세 분석
    if len(passed_list) > 0:
        story.append(PageBreak())
        story.append(Paragraph("Top Stocks Detailed Analysis", styles['KoreanHeading2']))
        
        for i, item in enumerate(passed_list[:min(10, len(passed_list))], 1):
            reasons = item.get("reasons", {})
            
            detail_text = f"""
            <b>#{i}: {item.get('code')} - {item.get('name', 'N/A')}</b><br/>
            Score: {item.get('score', 0):.2f}<br/>
            <br/>
            <b>Minervini Criteria:</b><br/>
            - RS Percentile: {item.get('rs_percentile', 0):.1f} {'(High)' if reasons.get('rs_high') else ''}<br/>
            - VCP Score: {item.get('vcp_score', 0):.2f} {'(Detected)' if reasons.get('vcp_detected') else ''}<br/>
            - Trend Template: {'Pass' if item.get('trend_ok') else 'Fail'}<br/>
            - Volume Contraction: {'Yes' if reasons.get('volume_contraction') else 'No'}<br/>
            - Base Formation: {reasons.get('base_type', 'N/A')}<br/>
            """
            
            story.append(Paragraph(detail_text, styles['KoreanBody']))
            story.append(Spacer(1, 0.2 * inch))
    
    doc.build(story)
    
    logger.info("[REPORT][MINERVINI][PDF] generated path=%s", output_path)
    return output_path


def generate_exit_analysis_pdf(
    *,
    code: str,
    entry_snapshot: Dict[str, Any],
    exit_snapshot: Dict[str, Any],
    comparison: Dict[str, Any],
    output_dir: Optional[Path] = None,
) -> Path:
    """
    매도 비교 분석 PDF 생성.
    
    Args:
        code: 종목코드
        entry_snapshot: 매수 당시 스냅샷
        exit_snapshot: 매도 시점 스냅샷
        comparison: 비교 분석 결과
        output_dir: 출력 디렉토리 (기본값: runtime/reports/exit/YYYY-MM-DD/)
    
    Returns:
        생성된 PDF 파일 경로
    """
    exit_date = exit_snapshot.get("exit_date", date.today())
    
    if output_dir is None:
        output_dir = Path("runtime/reports/exit") / exit_date.strftime("%Y-%m-%d")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{code}_exit_report.pdf"
    
    logger.info("[REPORT][EXIT][PDF] generating code=%s output=%s", code, output_path)
    
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        rightMargin=30,
        leftMargin=30,
        topMargin=30,
        bottomMargin=30,
    )
    
    story = []
    styles = _create_styles()
    
    # 제목
    story.append(Paragraph(f"Exit Analysis Report - {code}", styles['KoreanTitle']))
    story.append(Paragraph(f"Exit Date: {exit_date.strftime('%Y-%m-%d')}", styles['KoreanBody']))
    story.append(Spacer(1, 0.3 * inch))
    
    # 손익 요약
    story.append(Paragraph("P&L Summary", styles['KoreanHeading2']))
    pnl_text = f"""
    Entry Price: {entry_snapshot.get('entry_price', 0):,.0f} KRW<br/>
    Exit Price: {exit_snapshot.get('exit_price', 0):,.0f} KRW<br/>
    Quantity: {entry_snapshot.get('qty', 0):,}<br/>
    P&L: {exit_snapshot.get('pnl', 0):,.0f} KRW ({exit_snapshot.get('pnl_pct', 0):.2%})<br/>
    Hold Days: {exit_snapshot.get('hold_days', 0)}
    """
    story.append(Paragraph(pnl_text, styles['KoreanBody']))
    story.append(Spacer(1, 0.3 * inch))
    
    # 매수 당시 vs 현재 비교
    story.append(Paragraph("Entry vs Exit Comparison", styles['KoreanHeading2']))
    
    comparison_data = [
        ["Metric", "Entry", "Exit", "Change"]
    ]
    
    # 주요 지표 변화
    metrics = [
        ("RS Percentile", "rs_percentile", "rs_change"),
        ("VCP Score", "vcp_score", "vcp_change"),
        ("Trend OK", "trend_ok", "trend_lost"),
        ("Flow Score", "flow_score", "flow_deterioration"),
        ("MA50 Position", "ma50_position", "ma50_cross_down"),
    ]
    
    entry_features = entry_snapshot.get("features", {})
    
    for metric_name, entry_key, change_key in metrics:
        entry_val = entry_features.get(entry_key, "N/A")
        change_val = comparison.get(change_key, "N/A")
        
        # Exit 값 계산
        if isinstance(entry_val, (int, float)) and isinstance(change_val, (int, float)):
            exit_val = entry_val + change_val
        else:
            exit_val = "N/A"
        
        comparison_data.append([
            metric_name,
            str(entry_val),
            str(exit_val),
            str(change_val),
        ])
    
    table = Table(comparison_data, colWidths=[2*inch, 1.5*inch, 1.5*inch, 1.5*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    
    story.append(table)
    story.append(Spacer(1, 0.3 * inch))
    
    # 실패 원인 분석
    story.append(Paragraph("Exit Reason Analysis", styles['KoreanHeading2']))
    
    exit_reasons = []
    if comparison.get("vcp_broken"):
        exit_reasons.append("VCP pattern broken")
    if comparison.get("trend_lost"):
        exit_reasons.append("Trend template failed")
    if comparison.get("ma50_cross_down"):
        exit_reasons.append("Price crossed below MA50")
    if comparison.get("rs_change", 0) < -10:
        exit_reasons.append(f"RS deteriorated by {comparison.get('rs_change', 0):.0f}")
    if comparison.get("flow_deterioration", 0) < -0.05:
        exit_reasons.append("Institutional flow deteriorated")
    
    if exit_reasons:
        reasons_text = "<br/>".join([f"- {reason}" for reason in exit_reasons])
    else:
        reasons_text = "- Take profit target reached (planned exit)"
    
    story.append(Paragraph(reasons_text, styles['KoreanBody']))
    
    # PDF 빌드
    doc.build(story)
    
    logger.info("[REPORT][EXIT][PDF] generated path=%s", output_path)
    return output_path

