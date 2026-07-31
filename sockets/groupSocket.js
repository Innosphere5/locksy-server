import { v4 as uuidv4 } from "uuid";
import { db } from "../config/db.js";
import { users, groups, groupInvites, getUserData } from "../store/state.js";
import { syncGroupsToS3 } from "../services/s3Service.js";

export function handleGroupSocket(io, socket) {
  // ─── Group Management (REFINED) ─────────────────────
  socket.on("groep:create", (data) => {
    const { groupName: name, description, adminId, members: memberList } = data;
    const groupId = uuidv4();

    const adminUser = users.get(adminId) || getUserData(adminId);
    const adminEntry = {
      cid: adminId,
      nickname: adminUser?.nickname || 'Admin',
      role: 'ADMIN',
      encryptedKey: null // Admin has the raw key
    };

    const initialMembers = [adminEntry];
    const pendingInvitesList = [];

    (memberList || []).forEach(m => {
      if (!m || !m.cid || m.cid === adminId) return;
      const u = users.get(m.cid) || getUserData(m.cid);
      const memberEntry = {
        cid: m.cid,
        nickname: m.nickname || u?.nickname || `User-${m.cid.substring(0, 4)}`,
        avatar: m.avatar || u?.avatar || null,
        role: 'MEMBER',
        encryptedKey: m.encryptedKey || null
      };

      initialMembers.push(memberEntry);
      pendingInvitesList.push({
        cid: m.cid,
        nickname: memberEntry.nickname,
        encryptedKey: m.encryptedKey || null
      });
    });

    const groupObj = {
      groupId,
      name,
      description,
      groupLogo: data.groupLogo || null,
      adminId,
      adminPublicKey: adminUser?.publicKey || null,
      members: initialMembers,
      pendingInvites: pendingInvitesList,
      createdAt: new Date().toISOString(),
      messages: []
    };

    groups.set(groupId, groupObj);
    socket.join(`group_${groupId}`);

    // Sync to S3 (instead of db.json)
    syncGroupsToS3();

    console.log(`[Group] Created: ${name} (${groupId}) by ${adminId} with ${initialMembers.length} members`);

    // Send invitations to all pending members
    groupObj.pendingInvites.forEach(invitee => {
      const inviteData = {
        groupId,
        groupName: name,
        groupLogo: groupObj.groupLogo,
        description: description,
        fromNickname: adminEntry.nickname,
        adminId: adminId,
        adminPublicKey: adminUser?.publicKey || null,
        encryptedKey: invitee.encryptedKey,
        timestamp: new Date().toISOString(),
      };

      const targetUser = users.get(invitee.cid) || getUserData(invitee.cid);
      if (targetUser && targetUser.status === "online" && targetUser.socketId) {
        io.to(targetUser.socketId).emit("groep:invite", inviteData);
        console.log(`[Group] Sent real-time invite to ${invitee.cid}`);
      } else {
        const existing = groupInvites.get(invitee.cid) || [];
        existing.push(inviteData);
        groupInvites.set(invitee.cid, existing);

        // Persist invites to DB
        const allInvites = Array.from(groupInvites.entries()).map(([cid, invites]) => ({ cid, invites }));
        db.set('groupInvites', allInvites).write();

        console.log(`[Group] Buffered offline invite for ${invitee.cid}`);
      }
    });

    socket.emit("groep:created", { groupId, name, groupLogo: groupObj.groupLogo, members: initialMembers });
  });

  socket.on("groep:accept", (data) => {
    const { groupId, userId } = data; // userId is CID
    const group = groups.get(groupId);

    if (!group) return socket.emit("groep:error", { message: "Group not found" });

    // Find user in pendingInvites
    const inviteEntry = group.pendingInvites.find(m => m.cid === userId);
    if (!inviteEntry) {
      return socket.emit("groep:error", { message: "No pending invite for this user" });
    }

    // Move from pendingInvites to members
    group.pendingInvites = group.pendingInvites.filter(m => m.cid !== userId);

    const user = users.get(userId);
    const newMember = {
      cid: userId,
      nickname: user?.nickname || `User-${userId.substring(0, 4)}`,
      role: 'MEMBER',
      encryptedKey: inviteEntry.encryptedKey
    };

    group.members.push(newMember);

    // Sync to S3
    syncGroupsToS3();

    // Join the user to the socket room
    const targetSocketId = user?.socketId;
    if (targetSocketId) {
      const targetSocket = io.sockets.sockets.get(targetSocketId);
      if (targetSocket) {
        targetSocket.join(`group_${groupId}`);
        console.log(`[Group] User ${userId} accepted and joined room group_${groupId}`);
      }
    }

    // Notify the user they joined successfully
    socket.emit("groep:joined", {
      groupId,
      groupName: group.name,
      groupLogo: group.groupLogo || null,
      description: group.description || null,
      adminId: group.adminId,
      members: group.members
    });

    // Broadcast update to the group
    io.to(`group_${groupId}`).emit("group:update", {
      type: 'member_added',
      groupId,
      member: { cid: userId, nickname: newMember.nickname }
    });
  });

  socket.on("groep:invite:send", (data) => {
    const { groupId, adminCid, memberCid, encryptedKey } = data;
    const group = groups.get(groupId);

    if (!group) return socket.emit("groep:error", { message: "Group not found" });

    const admin = group.members.find(m => m.cid === adminCid && m.role === 'ADMIN');
    if (!admin) return socket.emit("groep:error", { message: "Unauthorized: Admin only" });

    // Add to pendingInvites if not already there or a member
    if (group.members.some(m => m.cid === memberCid)) return;
    if (!group.pendingInvites.some(m => m.cid === memberCid)) {
      group.pendingInvites.push({ cid: memberCid, encryptedKey });
      db.get('groups').find({ groupId }).assign({ pendingInvites: group.pendingInvites }).write();
    }

    const inviteData = {
      groupId,
      groupName: group.name,
      groupLogo: group.groupLogo || null,
      description: group.description || null,
      fromNickname: admin.nickname,
      adminId: adminCid,
      adminPublicKey: admin.publicKey || group.adminPublicKey,
      encryptedKey,
      timestamp: new Date().toISOString(),
    };

    const targetUser = users.get(memberCid);
    if (targetUser && targetUser.status === "online" && targetUser.socketId) {
      io.to(targetUser.socketId).emit("groep:invite", inviteData);
      console.log(`[Group] Invite sent to ${memberCid} for group ${group.name}`);
    } else {
      const existing = groupInvites.get(memberCid) || [];
      existing.push(inviteData);
      groupInvites.set(memberCid, existing);

      // Persist invites to DB
      const allInvites = Array.from(groupInvites.entries()).map(([cid, invites]) => ({ cid, invites }));
      db.set('groupInvites', allInvites).write();

      console.log(`[Group] Buffered offline invite for ${memberCid}`);
    }
  });

  socket.on("groep:remove_member", (data) => {
    const { groupId, adminCid, memberCid } = data;
    const group = groups.get(groupId);

    if (!group) return socket.emit("groep:error", { message: "Group not found" });

    // 1. Validate Admin
    if (group.adminId !== adminCid) {
      return socket.emit("groep:error", { message: "Unauthorized: Admin only" });
    }

    // Find the member to get their nickname before removing
    const removedMember = group.members.find(m => m.cid === memberCid);

    // 2. Remove Member
    group.members = group.members.filter(m => m.cid !== memberCid);

    // 3. Save & Sync
    syncGroupsToS3();

    // 4. Notify & Force Leave Socket Room
    io.to(`group_${groupId}`).emit("group:update", {
      type: 'member_removed',
      groupId,
      groupName: group.name,
      memberCid,
      memberNickname: removedMember?.nickname || 'A member'
    });

    // Find the socket of the removed member and make them leave the room
    const targetUser = users.get(memberCid);
    if (targetUser && targetUser.socketId) {
      const targetSocket = io.sockets.sockets.get(targetUser.socketId);
      if (targetSocket) {
        targetSocket.leave(`group_${groupId}`);
        targetSocket.emit("groep:removed", { groupId, groupName: group.name });
      }
    }
  });

  socket.on("groep:leave", (data) => {
    const { groupId, userId } = data;
    const group = groups.get(groupId);

    if (!group) return;

    // Find the member to get their nickname before removing
    const leftMember = group.members.find(m => m.cid === userId);

    // 1. Remove from members
    group.members = group.members.filter(m => m.cid !== userId);

    // 2. Handle Admin Leaving
    if (group.adminId === userId && group.members.length > 0) {
      // Pass admin rights to the first remaining member
      const newAdmin = group.members[0];
      group.adminId = newAdmin.cid;

      // FETCH NEW ADMIN'S PUBLIC KEY to ensure E2EE continues working
      const newAdminUser = users.get(newAdmin.cid);
      if (newAdminUser && newAdminUser.publicKey) {
        group.adminPublicKey = newAdminUser.publicKey;
      }
    }

    // 3. Delete group if empty
    if (group.members.length === 0) {
      groups.delete(groupId);
    }

    // 4. Save & Sync
    syncGroupsToS3();

    // 5. Notify
    socket.leave(`group_${groupId}`);
    io.to(`group_${groupId}`).emit("group:update", {
      type: 'member_left',
      groupId,
      memberCid: userId,
      memberNickname: leftMember?.nickname || 'A member',
      newAdminId: group.adminId
    });

    socket.emit("groep:left:success", { groupId });
  });

  socket.on("groep:update_logo", (data) => {
    const { groupId, adminCid, groupLogo } = data;
    const group = groups.get(groupId);

    if (!group) return socket.emit("groep:error", { message: "Group not found" });

    // Validate Admin
    if (group.adminId !== adminCid) {
      return socket.emit("groep:error", { message: "Unauthorized: Admin only" });
    }

    // Update memory & DB
    group.groupLogo = groupLogo;
    db.get('groups').find({ groupId }).assign({ groupLogo }).write();
    syncGroupsToS3();

    console.log(`[Group] Updated logo for group ${group.name} (${groupId})`);

    // Broadcast to room members
    io.to(`group_${groupId}`).emit("group:update", {
      type: "logo_updated",
      groupId,
      groupLogo
    });
  });

  socket.on("group:remove_member", (data) => {
    const { groupId, adminCid, memberCid } = data;
    const group = groups.get(groupId);

    if (!group) return socket.emit("group:error", { message: "Group not found" });

    const admin = group.members.find(m => m.cid === adminCid && m.role === 'ADMIN');
    if (!admin && adminCid !== memberCid) {
      return socket.emit("group:error", { message: "Unauthorized" });
    }

    group.members = group.members.filter(m => m.cid !== memberCid);

    // Save to DB
    db.get('groups').find({ groupId }).assign({ members: group.members }).write();

    // Notify removed member
    const user = users.get(memberCid);
    if (user && user.socketId) {
      io.to(user.socketId).emit("group:update", { type: 'removed_from_group', groupId });
      io.sockets.sockets.get(user.socketId)?.leave(groupId);
    }

    // Broadcast to others
    io.to(groupId).emit("group:update", { type: 'member_removed', groupId, memberCid });
  });

  socket.on("group:promote_admin", (data) => {
    const { groupId, adminCid, memberCid } = data;
    const group = groups.get(groupId);

    if (!group) return socket.emit("group:error", { message: "Group not found" });

    const admin = group.members.find(m => m.cid === adminCid && m.role === 'ADMIN');
    if (!admin) return socket.emit("group:error", { message: "Unauthorized: Admin only" });

    const member = group.members.find(m => m.cid === memberCid);
    if (member) {
      member.role = 'ADMIN';
      // Save to DB
      db.get('groups').find({ groupId }).assign({ members: group.members }).write();
      io.to(groupId).emit("group:update", { type: 'admin_promoted', groupId, memberCid });
    }
  });

  // ─── Group View-Once Opened Event ─────────────────────
  socket.on("group:open_view_once", (data) => {
    const { groupId, messageId, userCid } = data;
    const group = groups.get(groupId);
    if (!group) return;

    if (group.messages) {
      group.messages.forEach(m => {
        if (m.id === messageId) {
          m.isOpened = true;
          if (m.message && typeof m.message === 'object') {
            m.message.isOpened = true;
            m.message.uri = null;
            m.message.image = null;
            m.message.media_id = null;
            m.message.text = "[View Once Content Expired]";
          }
        }
      });
      syncGroupsToS3();
    }

    // Broadcast to room members (both group_groupId and groupId for compatibility)
    io.to(`group_${groupId}`).emit("message:opened", { roomId: groupId, groupId, messageId, userCid });
    io.to(groupId).emit("message:opened", { roomId: groupId, groupId, messageId, userCid });
    console.log(`[Group] View-once message ${messageId} opened in group ${groupId} by ${userCid}`);
  });
}
