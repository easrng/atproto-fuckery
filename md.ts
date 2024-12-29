import { dedent } from "ts-dedent";
// @deno-types="starry-night-types"
import {
  all,
  createStarryNight,
} from "starry-night";
import { toHtml } from "hast-util-to-html";
import { h } from "hastscript";
// @ts-ignore: lsp doesn't work well with json modules
import css from "starry-night-css" with {
  type: "json",
};
import type hast from "hast";
import { baseHeaders } from "./plc_serve.ts";

const starryNight = await createStarryNight(all);
function highlight(
  this: string,
  statics: TemplateStringsArray,
  ...holes: string[]
) {
  const rawText = dedent(statics, ...holes);
  const indent: string[] = [], offset: number[] = [], textLines: string[] = [];
  let i = 0;
  for (const line of rawText.split("\n")) {
    [, indent[i], textLines[i], { length: offset[i] }] = line.match(
      /^(\s*)(((?:[\-\/*|>][\s\-\/*|>]*)?).*)$/,
    )!;
    i++;
  }
  const nodes = starryNight.highlight(textLines.join("\n"), this);
  let line: (string | hast.Comment | hast.Doctype | hast.Element)[] = [""];
  const lines = [line];
  for (const node of nodes.children) {
    if (node.type === "text") {
      const text = node.value.split("\n");
      line.push(text.shift()!);
      while (text.length) {
        line = [""];
        lines.push(line);
        line.push(text.shift()!);
      }
    } else {
      line.push(node);
    }
  }
  return new Response(
    toHtml(
      h(
        "html",
        { lang: "en" },
        h(
          "head",
          h("title", "plc.easrng.net"),
          h("meta", {
            name: "viewport",
            content: "width=device-width, initial-scale=1",
          }),
          h(
            "style",
            css.text +
              dedent`
              html {
                white-space: pre-wrap;
                font-family: monospace;
                font-size: 0.85rem;
                color-scheme: dark light;
                line-height: 1.5;
                overflow-wrap: break-word;
              }

              div {
                padding-left: calc(var(--i) * 1ch);
                text-indent: calc(var(--o) * -1ch);
                min-height: 1.5em;
              }

              .s {
                position: absolute;
                width: 1px;
                height: 1px;
                padding: 0;
                margin: -1px;
                overflow: hidden;
                clip: rect(0, 0, 0, 0);
                white-space: pre;
                border-width: 0;
              }
              `,
          ),
        ),
        h(
          "body",
          lines.map((line, i) =>
            h(
              "div",
              { style: `--i:${indent[i].length + offset[i]};--o:${offset[i]}` },
              h("span", { class: "s" }, indent[i]),
              ...line,
            )
          ),
        ),
      ),
    ),
    {
      headers: {
        ...baseHeaders,
        "content-type": "text/html; charset=utf-8",
      },
    },
  );
}
export const md = highlight.bind("text.html.markdown.source.gfm.apib");
