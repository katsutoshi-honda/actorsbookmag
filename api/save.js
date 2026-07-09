// actorsbookmag CMS 保存API (Vercel Serverless Function)
// パスワードを照合し、サーバー側に隠したGITHUB_TOKENでnews.json(と背景画像)をコミットする。
// 必要な環境変数(Vercelに登録): GITHUB_TOKEN, EDIT_PASSWORD

const OWNER = "katsutoshi-honda";
const REPO = "actorsbookmag";
const BRANCH = "main";

module.exports = async (req, res) => {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "POSTのみ対応しています" });
  }

  const { password, record, image, thumb } = req.body || {};

  // 1) パスワード照合
  if (!process.env.EDIT_PASSWORD || password !== process.env.EDIT_PASSWORD) {
    return res.status(401).json({ error: "パスワードが違います" });
  }
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return res.status(500).json({ error: "サーバー未設定: GITHUB_TOKEN がありません" });
  }
  if (!record || !record.id) {
    return res.status(400).json({ error: "recordが不正です" });
  }
  // 保存のたびにSNS素材を作り直す（見出し/本文/背景の変更を反映）。
  // 公開記事なら push を検知してGitHub Actionsが再生成する。
  record.assets_ready = false;

  const api = (path, opts = {}) =>
    fetch(`https://api.github.com/repos/${OWNER}/${REPO}/contents/${path}`, {
      ...opts,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "actorsbookmag-cms",
        ...(opts.headers || {}),
      },
    });

  try {
    // 2) 背景画像があればコミットして record.background を更新
    if (image && image.dataB64 && image.ext) {
      const safeExt = String(image.ext).toLowerCase().replace(/[^a-z0-9]/g, "") || "jpg";
      const path = `images/news/bg/${record.id}-${Date.now()}.${safeExt}`;
      const r = await api(path, {
        method: "PUT",
        body: JSON.stringify({
          message: `cms: upload bg for ${record.id}`,
          content: image.dataB64,
          branch: BRANCH,
        }),
      });
      if (!r.ok) {
        const t = await r.text();
        return res.status(502).json({ error: "画像保存に失敗: " + t.slice(0, 200) });
      }
      record.background = "/" + path;
    }

    // 2.5) その場合成したニュースサムネ(Canvas)をthumbnailとして保存
    if (thumb) {
      const tpath = `images/news/${record.id}.jpg`;
      let tsha;
      const tcur = await api(`${tpath}?ref=${BRANCH}`);
      if (tcur.ok) tsha = (await tcur.json()).sha;
      const tr = await api(tpath, {
        method: "PUT",
        body: JSON.stringify({
          message: `cms: thumbnail ${record.id}`, content: thumb, branch: BRANCH, sha: tsha,
        }),
      });
      if (tr.ok) record.thumbnail = "/" + tpath;
    }

    // 3) 最新の news.json を取得(sha) → 該当recordを差し替え(なければ先頭に追加)
    const cur = await api(`data/news.json?ref=${BRANCH}`);
    let arr = [];
    let sha;
    if (cur.ok) {
      const j = await cur.json();
      sha = j.sha;
      arr = JSON.parse(Buffer.from(j.content, "base64").toString("utf-8"));
    }
    const idx = arr.findIndex((x) => x.id === record.id);
    if (idx >= 0) arr[idx] = record;
    else arr.unshift(record);

    // 4) news.json をコミット
    const content = Buffer.from(JSON.stringify(arr, null, 2), "utf-8").toString("base64");
    const put = await api("data/news.json", {
      method: "PUT",
      body: JSON.stringify({
        message: `cms: edit ${record.id}`,
        content,
        sha,
        branch: BRANCH,
      }),
    });
    if (!put.ok) {
      const t = await put.text();
      return res.status(502).json({ error: "保存に失敗: " + t.slice(0, 200) });
    }

    return res.status(200).json({ ok: true, background: record.background || "" });
  } catch (e) {
    return res.status(500).json({ error: String(e && e.message ? e.message : e) });
  }
};
