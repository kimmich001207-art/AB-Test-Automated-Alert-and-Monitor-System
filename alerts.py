"""Send XPC scheduled metric reports and revenue-loss alerts."""
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

import requests as _requests

from ads_quality.app.xpc_revenue_monitor import config, metadata_store
from ads_quality.app.xpc_revenue_monitor.ramp_policy import as_naive_datetime
from ads_quality.ml_infra.lib.slack import _get_slack_hook_url
from ads_quality.model.dnn.configs import ENABLED_PLACEMENT_TYPES


LOGGER = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


def _owner_tag(owner: str | None) -> str:
    """Return a Slack mention for an XPC owner, with a readable fallback."""
    if not owner:
        return ''
    slack_id = config.OWNER_SLACK_IDS.get(owner)
    return f' <@{slack_id}>' if slack_id else f' @{owner}'


def _abtest_title(abtest_id: int, abtest_name: str | None) -> str:
    """Return a readable AB test title with a fallback for old metadata."""
    return f'AB test {abtest_id}' + (f' - {abtest_name}' if abtest_name else '')


def _container_title(container_id: int | None, container_name: str | None) -> str:
    """Return a readable container title with a fallback for missing metadata."""
    label = 'unknown' if container_id is None else str(container_id)
    return f'Container {label}' + (f' - {container_name}' if container_name else '')


def _placement_scope_title(
    placement_ids: str | None,
    placement_id: int | None,
) -> str:
    if placement_ids:
        placement_ids = placement_ids.strip()

        if placement_ids == 'all placements':
            return placement_ids

        values = placement_ids.split(',')
    elif placement_id is not None:
        values = [str(placement_id)]
    else:
        return 'all placements'

    return ', '.join(
        ENABLED_PLACEMENT_TYPES.get(
            int(value.strip()),
            value.strip(),
        )
        for value in values
    )


def _placement_key(placement_id: Any) -> int | None:
    return None if placement_id is None else int(placement_id)


# -----------------------------------------------------------------------------
# Formatting helpers
# -----------------------------------------------------------------------------

def _fmt_decimal(value: Any) -> Any:
    return float(value) if isinstance(value, Decimal) else value


def _fmt_number(value: Any, digits: int = 4) -> str:
    value = _fmt_decimal(value)
    return f'{value:.{digits}f}' if isinstance(value, float) else str(value)


def _fmt_pct(value: Any) -> str:
    value = _fmt_decimal(value)
    return 'N/A' if value is None else f'{value:.2%}'


def _fmt_int(value: Any) -> str:
    value = _fmt_decimal(value)
    if value is None:
        return 'N/A'
    return f'{int(value):,}'


def _fmt_ratio(treatment_val: Any, control_val: Any) -> str:
    """Format a treatment-vs-control comparison like '0.0312 (-30.97% vs ctrl)'."""
    t = _fmt_decimal(treatment_val)
    c = _fmt_decimal(control_val)
    if t is None:
        return 'N/A'
    base = _fmt_number(treatment_val)
    if c is not None and isinstance(c, (int, float)) and c > 0:
        pct_change = (t - c) / c
        return f'{base} ({pct_change:+.2%} vs ctrl)'
    return base


# -----------------------------------------------------------------------------
# Slack
# -----------------------------------------------------------------------------

def _post_slack(message: str, channel: str) -> bool:
    """Post to Slack with response checking (shared notify_slack is fire-and-forget)."""
    payload = {'text': message, 'channel': channel, 'mrkdwn': True}
    response = _requests.post(config.ALERT_WEBHOOK_URL, json=payload)
    if not response.ok:
        LOGGER.error('Slack webhook returned %d: %s', response.status_code, response.text)
    return response.ok


# -----------------------------------------------------------------------------
# Cadence logic
# -----------------------------------------------------------------------------

def _report_cadence(
    xpc_start_ts: datetime,
    xpc_end_ts: datetime | None,
    run_ts: datetime,
    hourly_window: int = config.HOURLY_REPORT_HOURS,
    daily_hour: int = config.DAILY_REPORT_HOUR,
) -> str | None:
    """Determine if a business metric report should be sent this run.

    Returns 'hourly', 'daily', or None (skip this run).
    """
    # XPC already ended - no more reports
    if xpc_end_ts is not None and run_ts >= xpc_end_ts:
        return None

    hours_since_start = (run_ts - xpc_start_ts).total_seconds() / 3600
    if hours_since_start < 0:
        return None

    if hours_since_start < hourly_window:
        return 'hourly'

    if run_ts.hour == daily_hour:
        return 'daily'

    return None


def _cadence_label(cadence: str, xpc_start_ts: datetime, run_ts: datetime) -> str:
    """Human-readable label for the report cadence."""
    hours = int((run_ts - xpc_start_ts).total_seconds() / 3600)
    if cadence == 'hourly':
        return f'Hourly Report (hour {hours}/{config.HOURLY_REPORT_HOURS})'
    return f'Daily Report (day {hours // 24})'


# -----------------------------------------------------------------------------
# Message builders
# -----------------------------------------------------------------------------

def _version_message(row: Mapping[str, Any]) -> str:
    placement_id = _placement_key(row.get('placement_id'))
    abtest_id = int(row['abtest_id'])
    owner_tag = _owner_tag(row.get('owner'))
    placement_name = _placement_scope_title(
        row.get('placement_ids'), placement_id
    )
    abtest_title = _abtest_title(abtest_id, row.get('abtest_name'))
    return '\n'.join([
        f'*New XPC version started: {abtest_title}{owner_tag}*',
        f'  container: {_container_title(row.get("container_id"), row.get("container_name"))}',
        f'  placement: {placement_name}',
        f'  placement_id: {placement_id}',
        f'  version: {row["version"]}',
        f'  abtest_instance_id: {row["abtest_instance_id"]}',
        f'  start_ts: {row["start_ts"]}',
    ])


def _version_section(row: Mapping[str, Any]) -> str:
    """Format one new-version entry inside a container-level message."""
    placement_id = _placement_key(row.get('placement_id'))
    abtest_id = int(row['abtest_id'])
    placement_name = _placement_scope_title(
        row.get('placement_ids'), placement_id
    )
    abtest_title = _abtest_title(abtest_id, row.get('abtest_name'))
    placement_id_display = row.get('placement_id_display', placement_id)
    return '\n'.join([
        f'*{abtest_title}{_owner_tag(row.get("owner"))}*',
        f'  placement: {placement_name}',
        f'  placement_id: {placement_id_display}',
        f'  version: {row["version"]}',
        f'  abtest_instance_id: {row["abtest_instance_id"]}',
        f'  start_ts: {row["start_ts"]}',
    ])

def _merge_version_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Merge placement rows belonging to the same XPC version for display."""
    grouped: dict[tuple[int, int, int, Any], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (
            int(row['abtest_id']),
            int(row['version']),
            int(row['abtest_instance_id']),
            row['start_ts'],
        )
        grouped[key].append(row)

    merged_rows = []
    for version_rows in grouped.values():
        merged = dict(version_rows[0])
        placement_ids = sorted({
            _placement_key(row.get('placement_id'))
            for row in version_rows
            if row.get('placement_id') is not None
        })
        if any(row.get('placement_id') is None for row in version_rows):
            merged['placement_id'] = None
            merged['placement_ids'] = 'all placements'
            merged['placement_id_display'] = 'all'
        else:
            merged['placement_id'] = placement_ids[0] if placement_ids else None
            merged['placement_ids'] = ','.join(map(str, placement_ids))
            merged['placement_id_display'] = ','.join(map(str, placement_ids))
        merged_rows.append(merged)

    return sorted(
        merged_rows,
        key=lambda row: (
            int(row['abtest_id']),
            _placement_key(row.get('placement_id')) is not None,
            _placement_key(row.get('placement_id')) or -1,
        ),
    )


def _container_version_message(
    container_id: int | None,
    container_name: str | None,
    rows: Sequence[Mapping[str, Any]],
) -> str:
    """Build one message with one section per XPC version in a container."""
    sections = [
        _version_section(row)
        for row in _merge_version_rows(rows)
    ]
    return '\n'.join([
        f'*New XPC version started: {_container_title(container_id, container_name)}*',
        *sections,
    ])


def _option_block(row: Mapping[str, Any], control_row: Mapping[str, Any] | None) -> list[str]:
    """Format one option's metrics as indented lines."""
    option = row['test_option']
    is_ctrl = (int(option) == config.CONTROL_OPTION)
    label = 'control' if is_ctrl else 'treatment'

    lines = [f'  Option {option} ({label}):']
    lines.append(
        f'    traffic: {_fmt_pct(row["traffic_pct"])}'
        f' ({_fmt_int(row["users"])} / {_fmt_int(row["global_users"])} users)'
    )

    if is_ctrl or control_row is None:
        lines.append(f'    rev/user: {_fmt_number(row["rev_per_user"])}')
        lines.append(f'    clicks/user: {_fmt_number(row.get("clicks_per_user"))}')
        lines.append(f'    views/user: {_fmt_number(row.get("views_per_user"))}')
        lines.append(f'    impressions/user: {_fmt_number(row.get("impressions_per_user"))}')
    else:
        lines.append(f'    rev/user: {_fmt_ratio(row["rev_per_user"], control_row["rev_per_user"])}')
        lines.append(f'    clicks/user: {_fmt_ratio(row.get("clicks_per_user"), control_row.get("clicks_per_user"))}')
        lines.append(f'    views/user: {_fmt_ratio(row.get("views_per_user"), control_row.get("views_per_user"))}')
        lines.append(f'    impressions/user: {_fmt_ratio(row.get("impressions_per_user"), control_row.get("impressions_per_user"))}')

    return lines


def _business_metric_message(
    placement_id: int | None,
    abtest_id: int,
    version: int,
    cadence: str,
    xpc_start_ts: datetime,
    run_ts: datetime,
    option_rows: Sequence[Mapping[str, Any]],
    owner: str | None = None,
    abtest_name: str | None = None,
    placement_ids: str | None = None,
) -> str:
    """Build a full business metric report for one XPC."""
    placement_name = _placement_scope_title(placement_ids, placement_id)
    owner_tag = _owner_tag(owner)
    label = _cadence_label(cadence, xpc_start_ts, run_ts)
    abtest_title = _abtest_title(abtest_id, abtest_name)

    # Find control row
    control_row = None
    for r in option_rows:
        if int(r['test_option']) == config.CONTROL_OPTION:
            control_row = r
            break

    window = ''
    if option_rows:
        window = f'{option_rows[0].get("start_ts")} -> {option_rows[0].get("end_ts")}'

    lines = [
        f'*[{label}] {abtest_title}{owner_tag}*',
        f'  placement: {placement_name} | version: {version} | window: {window}',
        '',
    ]

    # Control first, then treatments sorted by option
    sorted_rows = sorted(option_rows, key=lambda r: (int(r['test_option']) != config.CONTROL_OPTION, int(r['test_option'])))
    for r in sorted_rows:
        lines.extend(_option_block(r, control_row))

    return '\n'.join(lines)


def _container_metric_message(
    container_id: int | None,
    container_name: str | None,
    sections: Sequence[str],
) -> str:
    """Combine cadence-eligible AB test reports under one container heading."""
    return '\n\n'.join([
        f'*{_container_title(container_id, container_name)}*',
        *sections,
    ])


def _revenue_loss_message(
    row: Mapping[str, Any],
    control_row: Mapping[str, Any] | None,
    owner: str | None = None,
    abtest_name: str | None = None,
    container_id: int | None = None,
    container_name: str | None = None,
    placement_ids: str | None = None,
    revenue_drop_threshold: float | None = None,
) -> str:
    """Build a personal revenue-loss alert message."""
    placement_id = _placement_key(row.get('placement_id'))
    abtest_id = int(row['abtest_id'])
    owner_tag = _owner_tag(owner or row.get('owner'))
    placement_name = _placement_scope_title(
        placement_ids or row.get('placement_ids'), placement_id
    )
    abtest_title = _abtest_title(abtest_id, abtest_name or row.get('abtest_name'))
    container_label = _container_title(
        container_id if container_id is not None else row.get('container_id'),
        container_name or row.get('container_name'),
    )

    lines = [
        f'*REVENUE LOSS ALERT: {abtest_title}{owner_tag}*',
        f'  container: {container_label}',
        f'  placement: {placement_name}',
        f'  version: {row.get("version")}',
        f'  option: {row["test_option"]}',
        f'  rev_drop: {_fmt_pct(row["rev_drop"])} '
        f'(threshold: {_fmt_pct(revenue_drop_threshold if revenue_drop_threshold is not None else config.REVENUE_DROP_THRESHOLD)})',
        f'  rev/user: {_fmt_ratio(row["rev_per_user"], control_row["rev_per_user"] if control_row else None)}',
        f'  traffic: {_fmt_pct(row["traffic_pct"])} ({_fmt_int(row["users"])} users)',
        f'  window: {row.get("start_ts")} -> {row.get("end_ts")}',
    ]
    return '\n'.join(lines)


def _revenue_alert_policy(row: Mapping[str, Any]) -> tuple[bool, float]:
    """Return whether a treatment row can alert and its effective threshold."""
    traffic_pct = row.get('traffic_pct')
    if traffic_pct is None:
        return False, config.REVENUE_DROP_THRESHOLD

    traffic_pct = float(traffic_pct)
    if traffic_pct < config.VERY_LOW_TRAFFIC_THRESHOLD:
        return False, config.LOW_TRAFFIC_REVENUE_DROP_THRESHOLD
    if traffic_pct < config.LOW_TRAFFIC_THRESHOLD:
        return True, config.LOW_TRAFFIC_REVENUE_DROP_THRESHOLD
    return True, config.REVENUE_DROP_THRESHOLD


# -----------------------------------------------------------------------------
# New version alerts (kept from previous implementation)
# -----------------------------------------------------------------------------

def _send_new_version_alerts(
    metadata_table: str,
    run_id: str,
    sent_at: datetime,
) -> tuple[int, int, list[dict[str, Any]]]:
    """Send new-version alerts and return (sent, failed, current_versions)."""
    current_versions = metadata_store.fetch_current_versions(metadata_table)
    sent_keys = metadata_store.fetch_new_version_alert_keys(metadata_table)
    sent = 0
    failed = 0
    new_versions_by_abtest: dict[
        tuple[int | None, str | None, int, int, int], list[dict[str, Any]]
    ] = defaultdict(list)
    for row in current_versions:
        if metadata_store.alert_key(row) in sent_keys:
            continue
        abtest_key = (
            row.get('container_id'),
            row.get('container_name'),
            int(row['abtest_id']),
            int(row['version']),
            int(row['abtest_instance_id']),
        )
        new_versions_by_abtest[abtest_key].append(row)

    for (container_id, container_name, _, _, _), rows in sorted(
        new_versions_by_abtest.items(),
        key=lambda item: (
            item[0][0] is None,
            item[0][0] or -1,
            item[0][1] or '',
            item[0][2],
            item[0][3],
            item[0][4],
        ),
    ):
        rows.sort(
            key=lambda row: (
                int(row['abtest_id']),
                _placement_key(row.get('placement_id')) is not None,
                _placement_key(row.get('placement_id')) or -1,
            )
        )
        message = _container_version_message(container_id, container_name, rows)
        if not _post_slack(message, config.ALERT_CHANNEL):
            failed += 1
            continue

        for row in rows:
            key = metadata_store.alert_key(row)
            start_ts = as_naive_datetime(row['start_ts'])
            end_ts = None if row['end_ts'] is None else as_naive_datetime(row['end_ts'])
            metadata_store.record_alert(
                metadata_table, 'new_version', key, run_id, sent_at,
                placement_id=_placement_key(row.get('placement_id')),
                abtest_id=int(row['abtest_id']),
                version=int(row['version']),
                owner=row.get('owner'),
                abtest_instance_id=int(row['abtest_instance_id']),
                start_ts=start_ts,
                end_ts=end_ts,
                container_id=container_id,
                container_name=container_name,
                abtest_name=row.get('abtest_name'),
            )
            sent_keys.add(key)
        sent += 1
    return sent, failed, current_versions


# -----------------------------------------------------------------------------
# Main entry point
# -----------------------------------------------------------------------------

def send_alerts(
    result_table: str,
    metadata_table: str,
    run_ts_kst: str,
    run_id: str,
    **_: Any,
) -> dict[str, int]:
    run_ts = datetime.strptime(run_ts_kst, '%Y%m%d%H%M%S').replace(tzinfo=KST)
    run_ts_naive = run_ts.replace(tzinfo=None)
    run_date = run_ts_kst[:8]

    # Step 1: Send new version starting events
    new_sent, new_failed, current_versions = _send_new_version_alerts(
        metadata_table, run_id, run_ts_naive
    )

    # Step 2: Build version metadata lookup (reuse data from step 1)
    version_meta = {}
    for v in current_versions:
        k = (v.get('container_id'), int(v['abtest_id']))
        version_meta[k] = v

    # Step 3: Fetch all metric results for this run
    all_rows = metadata_store.fetch_all_results(result_table, run_date)

    # Group merged results by (container_id, abtest_id). Each row already
    # represents all configured placements for one AB test and option.
    grouped: dict[tuple[int | None, int], list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        k = (row.get('container_id'), int(row['abtest_id']))
        grouped[k].append(row)

    # Step 4: Load sent report keys for deduplication
    sent_report_keys = metadata_store.fetch_sent_report_keys(metadata_table)

    sent = new_sent
    failed = new_failed

    # Step 5: Build cadence-eligible metric sections by container. Revenue-loss
    # alerts remain individual messages and are evaluated in the same loop.
    report_groups: dict[tuple[int | None, str | None], list[dict[str, Any]]] = defaultdict(list)
    for (container_id, abtest_id), option_rows in grouped.items():
        meta = version_meta.get((container_id, abtest_id))
        if meta is None:
            continue

        xpc_start_ts = as_naive_datetime(meta['start_ts'])
        xpc_end_ts = (
            None if meta['end_ts'] is None
            else as_naive_datetime(meta['end_ts'])
        )
        version = int(meta['version'])
        cadence = _report_cadence(xpc_start_ts, xpc_end_ts, run_ts_naive)

        if cadence is not None:
            container_key = (meta.get('container_id'), meta.get('container_name'))
            report_groups[container_key].append({
                'placement_id': _placement_key(option_rows[0].get('placement_id')),
                'placement_ids': option_rows[0].get('placement_ids'),
                'abtest_id': abtest_id,
                'version': version,
                'cadence': cadence,
                'xpc_start_ts': xpc_start_ts,
                'option_rows': option_rows,
                'owner': meta.get('owner'),
                'abtest_name': meta.get('abtest_name'),
                'container_id': meta.get('container_id'),
                'container_name': meta.get('container_name'),
            })

        # If any treatment has revenue loss > threshold, send a personal alert.
        control_row = next(
            (r for r in option_rows if int(r['test_option']) == config.CONTROL_OPTION),
            None,
        )
        for r in option_rows:
            if int(r['test_option']) == config.CONTROL_OPTION:
                continue
            rev_drop = r.get('rev_drop')
            should_send, revenue_drop_threshold = _revenue_alert_policy(r)
            if (
                not should_send
                or rev_drop is None
                or float(rev_drop) <= revenue_drop_threshold
            ):
                continue
            alert_msg = _revenue_loss_message(
                r,
                control_row,
                owner=meta.get('owner'),
                abtest_name=meta.get('abtest_name'),
                container_id=meta.get('container_id'),
                container_name=meta.get('container_name'),
                placement_ids=r.get('placement_ids'),
                revenue_drop_threshold=revenue_drop_threshold,
            )
            if _post_slack(alert_msg, config.ALERT_CHANNEL):
                sent += 1
            else:
                failed += 1

    # Step 6: Send one deduplicated report per container.
    for (container_id, container_name), sections in sorted(
        report_groups.items(), key=lambda item: (item[0][0] is None, item[0][0] or -1)
    ):
        sections.sort(
            key=lambda section: (
                section['abtest_id'],
                section['placement_id'] is not None,
                section['placement_id'] or -1,
            )
        )
        if container_id is None:
            container_key = f'unknown:{container_name or ""}'
            rpt_key = f'report:container:{container_key}:{run_ts_kst}'
        else:
            rpt_key = metadata_store.container_report_key(int(container_id), run_ts_kst)
        if rpt_key in sent_report_keys:
            LOGGER.info('Skipping duplicate report %s', rpt_key)
            continue

        messages = [
            _business_metric_message(
                section['placement_id'],
                section['abtest_id'],
                section['version'],
                section['cadence'],
                section['xpc_start_ts'],
                run_ts_naive,
                section['option_rows'],
                owner=section['owner'],
                abtest_name=section['abtest_name'],
                placement_ids=section['placement_ids'],
            )
            for section in sections
        ]
        msg = _container_metric_message(container_id, container_name, messages)
        if _post_slack(msg, config.ALERT_CHANNEL):
            first = sections[0]
            metadata_store.record_alert(
                metadata_table, 'report', rpt_key, run_id, run_ts_naive,
                placement_id=first['placement_id'],
                abtest_id=first['abtest_id'],
                version=first['version'],
                owner=first['owner'],
                container_id=container_id,
                container_name=container_name,
                abtest_name=first['abtest_name'],
            )
            sent_report_keys.add(rpt_key)
            sent += 1
        else:
            failed += 1

    LOGGER.info('Sent %d, failed %d XPC alerts/reports', sent, failed)
    if failed:
        raise RuntimeError(f'{failed} XPC alert(s)/report(s) failed to deliver to Slack')
    return {'sent': sent, 'failed': failed}
