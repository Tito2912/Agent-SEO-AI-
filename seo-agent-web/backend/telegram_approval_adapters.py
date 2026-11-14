"""Closed set of owner actions. Telegram text is never executable input."""

from datetime import timedelta
from sqlalchemy import select

try:
    from . import own_seo as s, own_writing as w, seo_watch, telegram_approval_format as display
    from .models import (
        OwnSeoBrief,
        OwnSeoDraft,
        SeoKnowledgeSource,
        SeoKnowledgeRevision,
        TelegramApprovalRequest,
    )
except ImportError:
    import own_seo as s
    import own_writing as w
    import seo_watch
    import telegram_approval_format as display
    from models import (
        OwnSeoBrief,
        OwnSeoDraft,
        SeoKnowledgeSource,
        SeoKnowledgeRevision,
        TelegramApprovalRequest,
    )


def document(
    kind,
    target,
    version,
    title,
    body,
    effect,
    path,
    ready=True,
    *,
    sections=None,
    summary="",
    en=False,
):
    doc = dict(
        kind=kind,
        target=target,
        version=version,
        title=title,
        effect=effect,
        path=path,
        ready=ready,
    )
    return display.document(
        doc,
        [
            ("📌 Topic" if en else "📌 Sujet", title),
            ("🎯 Your decision" if en else "🎯 Ce que tu valides", effect),
        ]
        + (sections if sections is not None else [("📄 Dossier", body)]),
        summary=summary or title,
    )


def inspect(db, owner, kind, target, lang):
    s.actor(db, owner)
    if kind in {'social_render', 'social_review'}:
        try:
            from . import own_social_media
        except ImportError:
            import own_social_media
        return own_social_media.approval_document(db, owner, target, kind, lang)
    if kind in {'support_write','support_send','support_review'}:
        try:
            from . import own_support
        except ImportError:
            import own_support
        return own_support.approval_document(db, owner, target, kind, lang)
    if kind in {'blog_pr', 'blog_publish'}:
        try:
            from . import own_blog
        except ImportError:
            import own_blog
        return own_blog.approval_document(db, owner, target, kind, lang)
    en = lang == "en"
    if kind == "knowledge":
        row = db.get(SeoKnowledgeSource, target)
        source = seo_watch.REGISTRY.get(target)
        if (
            not source
            or not row
            or row.lease_token
            or seo_watch.status(row, now=s.clock())
            not in {"changed", "review_required"}
        ):
            raise ValueError("changed")
        revision = db.get(SeoKnowledgeRevision, (target, row.latest_hash))
        if not revision or revision.url != source.url:
            raise ValueError("changed")
        value = [row.latest_hash, row.guidance_fr, row.guidance_en]
        ready = all(20 <= len(v.strip()) <= 1200 for v in value[1:])
        body = (
            source.url
            + "\nUTC : "
            + seo_watch.utc(row.checked_at).isoformat()
            + "\nVersion : "
            + row.latest_hash[:12]
            + "\n"
            + revision.excerpt
            + "\n\nFR : "
            + row.guidance_fr
            + "\n\nEN : "
            + row.guidance_en
        )
        effect = (
            "Approve these exact FR/EN summaries for this source version. Reject keeps this version unapproved."
            if en
            else "Approuver ces résumés FR/EN pour cette version de la source. Refuser laisse cette version non validée."
        )
        if not ready:
            effect += (
                "\nSummaries must first be completed in Noyaru."
                if en
                else "\nCompléter d’abord les résumés dans Noyaru."
            )
        return document(
            kind,
            target,
            s.digest(value),
            source.title_en if en else source.title,
            body,
            effect,
            "/settings/system/seo-knowledge",
            ready,
            en=en,
        )
    if kind in {"brief", "writing"}:
        workspace, _, row = s.brief_for(db, owner, target)
        if workspace.paused:
            raise ValueError("paused")
        state = s.view(db, owner, lang=row.lang)
        if row.candidate_key not in {c["key"] for c in state["candidates"]}:
            raise ValueError("changed")
        if kind == "brief":
            if row.status != "prepared":
                raise ValueError("changed")
            value = [
                workspace.token,
                row.id,
                row.candidate_key,
                row.title,
                row.evidence,
                row.lang,
            ]
            return document(
                kind,
                target,
                s.digest(value),
                row.title,
                "",
                "Approve the brief only. Reject discards it. No AI request or publication."
                if en
                else "Valider le brief uniquement. Refuser l’abandonne. Aucun appel IA ni publication.",
                "/settings/system/own-seo",
                sections=display.brief_sections(row, en),
                en=en,
            )
        value, fingerprint = w.bundle(db, owner, target, allow_prepared=True)
        legacy_decision=db.scalar(select(TelegramApprovalRequest.id).where(
            TelegramApprovalRequest.owner_id==owner,
            TelegramApprovalRequest.kind=='writing',
            TelegramApprovalRequest.target_id==target,
            TelegramApprovalRequest.version==fingerprint,
            TelegramApprovalRequest.state.in_({'approved','rejected','expired'}),
        ).limit(1))
        if legacy_decision:
            # A presentation upgrade must not revive refused/expired work or
            # widen a durably accepted legacy writing request.
            raise ValueError('changed')
        previous = db.scalar(
            select(OwnSeoDraft.id)
            .where(
                OwnSeoDraft.owner_id == owner,
                OwnSeoDraft.brief_id == target,
                OwnSeoDraft.input_hash == fingerprint,
                OwnSeoDraft.status.in_(
                    {"queued", "running", "draft", "reviewed", "uncertain"}
                ),
            )
            .limit(1)
        )
        if previous:
            raise ValueError("changed")
        effect = (
            "Authorize one paid AI text request, valid for 24 hours, within the existing two requests / rolling 24h cap. "
            "Only the dossier below is sent to the configured provider. Private draft, no publication."
            if en
            else "Décision 1/2 : valider ce sujet et autoriser un appel IA texte payant, valable 24 h, dans la limite existante de deux appels / 24 h glissantes. "
            "Seul le dossier ci-dessous est transmis au fournisseur configuré. Brouillon privé, aucune publication."
        )
        effect += (
            '\nApprove the brief and writing together. The publication proposal is prepared read-only; you will review the article in decision 2/2 before any GitHub write or publication.'
            if en else
            '\nLe sujet et la rédaction sont validés ensemble. La proposition de publication se prépare en lecture seule ; tu reliras l’article lors de la décision 2/2 avant toute écriture GitHub ou publication.'
        )
        doc = document(
            kind,
            target,
            fingerprint,
            row.title,
            "",
            effect,
            w.PATH + "?brief=" + target,
            sections=display.brief_sections(row, en) + display.writing_sections(value, en),
            en=en,
        )
        # Separate version from legacy writing cards: an old decision keeps its
        # original scope, even though the content fingerprint stays compatible.
        doc['version'] = s.digest(['article-writing-v2', fingerprint, row.title, row.evidence])
        doc['stage'] = 1
        return doc
    if kind == "draft":
        row = w.row_for(db, owner, target)
        if (
            row.status != "draft"
            or not w.current(db, row)
            or not row.content_hash
            or row.content_hash != s.digest(row.content)
        ):
            raise ValueError("changed")
        return document(
            kind,
            target,
            s.digest([row.input_hash, row.content_hash]),
            row.content["title"],
            "",
            "Confirm that every claim is supported by its sources. Reject cancels the draft. No publication."
            if en
            else "Confirmer que chaque affirmation est étayée par ses sources. Refuser annule le brouillon. Aucune publication.",
            w.PATH + "?brief=" + row.brief_id,
            sections=display.draft_sections(row, en),
            en=en,
        )
    raise ValueError("unsupported")


def candidates(db, owner, lang):
    # Limits bound a sweep, not authority. Older rows are still checked on every click.
    try:
        from . import own_social_media
    except ImportError:
        import own_social_media
    yield from own_social_media.candidates(db, owner)
    try:
        from .models import OwnBlogPublication
    except ImportError:
        from models import OwnBlogPublication
    for row in db.scalars(select(OwnBlogPublication).where(OwnBlogPublication.owner_id == owner,
                         OwnBlogPublication.state.in_({'proposal', 'review_ready'})).order_by(OwnBlogPublication.created_at).limit(20)):
        yield ('blog_pr' if row.state == 'proposal' else 'blog_publish'), row.id
    try:
        from .models import OwnSupportChannel, OwnSupportTicket
    except ImportError:
        from models import OwnSupportChannel, OwnSupportTicket
    for row in db.scalars(select(OwnSupportTicket).where(OwnSupportTicket.owner_id==owner,
        OwnSupportTicket.binding.in_(select(OwnSupportChannel.binding).where(OwnSupportChannel.owner_id==owner,OwnSupportChannel.active.is_(True))),
        OwnSupportTicket.created_at>s.clock()-timedelta(days=30),
        OwnSupportTicket.state.in_({'received','draft','review_required','send_uncertain'})).order_by(OwnSupportTicket.created_at).limit(20)):
        if row.state=='send_uncertain' and row.receipt.get('review_ack'):continue
        yield {'received':'support_write','draft':'support_send','review_required':'support_review','send_uncertain':'support_review'}[row.state], row.id
    for key in seo_watch.REGISTRY:
        yield "knowledge", key
    for row in db.scalars(
        select(OwnSeoBrief)
        .where(
            OwnSeoBrief.owner_id == owner,
            OwnSeoBrief.status.in_({"prepared", "reviewed"}),
        )
        .order_by(OwnSeoBrief.created_at.desc())
        .limit(40)
    ):
        if row.kind in {'research','guide'}:
            yield 'writing', row.id
        elif row.status == 'prepared':
            yield 'brief', row.id
    # Draft review is part of the final article decision. Do not emit another
    # card simply to mark the same text reviewed.


def apply(db, owner, kind, target, approve, key, version):
    actor = s.actor(db, owner)
    if kind in {'social_render', 'social_review'}:
        try:
            from . import own_social_media
        except ImportError:
            import own_social_media
        return own_social_media.decide(db, owner, target, kind, approve, version)
    if kind in {'support_write','support_send','support_review'}:
        try:
            from . import own_support
        except ImportError:
            import own_support
        return own_support.decide(db, owner, target, kind, approve, version)
    if kind in {'blog_pr', 'blog_publish'}:
        try:
            from . import own_blog
        except ImportError:
            import own_blog
        return own_blog.decision_in_session(db, owner, target, kind, approve, version)
    if kind == "test":
        return None
    if kind == "knowledge":
        row = db.get(SeoKnowledgeSource, target)
        if approve:
            # Web source reviews use their own transaction. Lock and reload before
            # applying this exact Telegram version, including both editable summaries.
            db.refresh(row, with_for_update=True)
            if s.digest([row.latest_hash, row.guidance_fr, row.guidance_en]) != version:
                raise ValueError("changed")
            seo_watch.review(
                db,
                target,
                row.latest_hash,
                fr=row.guidance_fr,
                en=row.guidance_en,
                actor=actor,
                now=s.clock(),
            )
    elif kind == "brief":
        workspace, _, _ = s.brief_for(db, owner, target)
        s.review_in_session(
            db,
            owner,
            target,
            workspace.token,
            confirm=True,
            status="reviewed" if approve else "discarded",
        )
    elif kind == "writing":
        if approve:
            doc=inspect(db, owner, kind, target, 'fr')
            if doc['version'] != version:raise ValueError('changed')
            fingerprint=w.bundle(db, owner, target, allow_prepared=True)[1]
            return w.authorize_in_session(db, owner, target, fingerprint, key, True, article_journey=True)
        workspace, _, _ = s.brief_for(db, owner, target)
        s.review_in_session(db, owner, target, workspace.token, confirm=True, status='discarded')
    elif kind == "draft":
        row = w.row_for(db, owner, target)
        if approve:
            w.review_in_session(db, owner, target, row.content_hash, True)
        else:
            w.cancel_in_session(db, owner, target)
    else:
        raise ValueError("unsupported")
    return None
