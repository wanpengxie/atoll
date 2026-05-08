import { BaseTool } from '../base-tool';
import type { ToolResult } from 'coagent-xhs-shared';
import { DOUYIN_CREATOR_URLS, DOUYIN_TIMEOUTS } from './selectors';

interface DouyinCreatorInfoResult {
  success: boolean;
  data?: {
    username: string;
    user_id: string;
    avatar_url: string;
    follower_count: number;
    following_count: number;
    work_count: number;
    like_count: number;
    recent_works?: Array<{
      work_id: string;
      title: string;
      cover_url: string;
      play_count: number;
      like_count: number;
      comment_count: number;
      share_count: number;
      post_time: string;
    }>;
  };
  error?: string;
}

/**
 * douyin_get_creator_info - 获取抖音创作者账号数据
 *
 * 工作流程：
 * 1. 导航到抖音创作者首页
 * 2. 从页面 DOM 提取数据统计
 * 3. 返回结构化 JSON
 */
export class DouyinGetCreatorInfoTool extends BaseTool {
  name = 'douyin_get_creator_info';

  async execute(): Promise<ToolResult> {
    try {
      // 1. 查找或创建抖音标签页
      const tab = await this.findOrCreateDouyinTab(DOUYIN_CREATOR_URLS.HOME);

      if (!tab.id) {
        throw new Error('无法创建标签页');
      }

      // 2. 等待页面加载完成
      await this.waitForTabLoad(tab.id, DOUYIN_TIMEOUTS.PAGE_LOAD);

      // 3. 额外等待，确保页面数据加载
      await new Promise((resolve) => setTimeout(resolve, 3000));

      // 4. 在页面中执行数据提取
      const result = await this.executeInTab<DouyinCreatorInfoResult>(
        tab.id,
        this.extractCreatorInfo
      );

      if (!result.success) {
        return this.createErrorResult(result.error || '获取抖音创作者数据失败');
      }

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2),
          },
        ],
        isError: false,
      };
    } catch (error) {
      console.error('[douyin_get_creator_info] Error:', error);
      return this.createErrorResult(
        error instanceof Error ? error.message : '获取抖音创作者数据失败'
      );
    }
  }

  /**
   * 在页面中执行的数据提取函数
   */
  private extractCreatorInfo = (): DouyinCreatorInfoResult => {
    try {
      // 提取用户名
      const usernameElement = document.querySelector(
        '.user-name, .creator-name, [class*="userName"], [class*="nickName"]'
      );
      const username = usernameElement?.textContent?.trim() || '';

      // 提取头像
      const avatarElement = document.querySelector(
        '.avatar img, [class*="avatar"] img, [class*="userAvatar"] img'
      ) as HTMLImageElement | null;
      const avatar_url = avatarElement?.src || '';

      // 提取统计数据
      let follower_count = 0;
      let following_count = 0;
      let work_count = 0;
      let like_count = 0;

      // 尝试从统计项中提取数据
      const statsElements = document.querySelectorAll(
        '.stats-item, .data-item, [class*="dataItem"], [class*="statsItem"], [class*="statItem"]'
      );

      statsElements.forEach((element) => {
        const text = element.textContent || '';
        const label = text.toLowerCase();

        // 提取数字
        const numberMatch = text.match(/[\d,]+\.?\d*/);
        let number = 0;
        if (numberMatch) {
          const numStr = numberMatch[0].replace(/,/g, '');
          number = parseFloat(numStr);

          // 处理单位（万、亿）
          if (text.includes('万') || text.includes('w')) {
            number *= 10000;
          } else if (text.includes('亿')) {
            number *= 100000000;
          }
          number = Math.round(number);
        }

        if (label.includes('粉丝') || label.includes('follower') || label.includes('fans')) {
          follower_count = number;
        } else if (label.includes('关注') || label.includes('following')) {
          following_count = number;
        } else if (label.includes('作品') || label.includes('work') || label.includes('视频')) {
          work_count = number;
        } else if (label.includes('获赞') || label.includes('like') || label.includes('点赞')) {
          like_count = number;
        }
      });

      // 尝试从页面其他位置获取数据（备选方案）
      if (follower_count === 0) {
        const allText = document.body.innerText;
        const fansMatch = allText.match(/粉丝[：:\s]*([0-9,.]+[万亿]?)/);
        if (fansMatch) {
          let num = parseFloat(fansMatch[1].replace(/,/g, ''));
          if (fansMatch[1].includes('万')) num *= 10000;
          if (fansMatch[1].includes('亿')) num *= 100000000;
          follower_count = Math.round(num);
        }
      }

      // 尝试从 window 对象获取数据（如果页面有预加载数据）
      const windowData = (window as any).__INITIAL_STATE__ || (window as any).__NUXT__;
      if (windowData?.user || windowData?.data?.user) {
        const userData = windowData.user || windowData.data?.user;
        if (userData.nickname) {
          return {
            success: true,
            data: {
              username: userData.nickname || username,
              user_id: userData.uid || userData.userId || '',
              avatar_url: userData.avatar || avatar_url,
              follower_count: userData.followerCount || userData.fans || follower_count,
              following_count: userData.followingCount || userData.following || following_count,
              work_count: userData.workCount || userData.awemeCount || work_count,
              like_count: userData.totalFavorited || userData.likeCount || like_count,
            },
          };
        }
      }

      // 如果没有获取到任何数据，返回错误
      if (!username && follower_count === 0 && work_count === 0) {
        throw new Error('无法提取创作者数据，请确保已登录抖音创作者平台');
      }

      return {
        success: true,
        data: {
          username,
          user_id: '', // 需要从其他地方获取
          avatar_url,
          follower_count,
          following_count,
          work_count,
          like_count,
        },
      };
    } catch (error) {
      console.error('[extractCreatorInfo] Error:', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : '数据提取失败',
      };
    }
  };
}
