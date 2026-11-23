// Compile-time examples of the public binary photo/retained-input API.
declare const photoApi: DesktopE2eeApi;
declare const photoContext: number;
declare const selectedPhoto: File;
const photoTarget = { conversationId: '11111111-1111-4111-8111-111111111111', messageId: '22222222-2222-4222-8222-222222222222' };
const newPhoto: DesktopE2eePhoto = { ...photoTarget, file: selectedPhoto, caption: '', choice: { mode: 'clean', quality: null, maxDimension: 0 } };
const photoPrepared: Promise<DesktopAttachmentReply> = photoApi.preparePhoto(newPhoto, photoContext);
photoApi.attachmentInput(photoTarget, photoContext);
photoApi.preparePhoto({ ...newPhoto, choice: { mode: 'original', metadataWarningAcknowledged: true } }, photoContext);
// @ts-expect-error A draft belongs to its observed native account context.
photoApi.preparePhoto(newPhoto);
// @ts-expect-error Retained lookup must also bind an observed account context.
photoApi.attachmentInput(photoTarget);
// @ts-expect-error An older Original preference is not metadata-retention consent.
photoApi.preparePhoto({ ...newPhoto, choice: { mode: 'original' } }, photoContext);
// @ts-expect-error IPC accepts selected bytes, not a renderer-selected local path.
photoApi.preparePhoto({ ...newPhoto, path: 'private/file.png' }, photoContext);
// @ts-expect-error The metadata-clean flag is derived by native preprocessing.
photoApi.preparePhoto({ ...newPhoto, photoMetadataStripped: true }, photoContext);
async function consumePhotoReply() {
  const { attachment } = await photoPrepared;
  if (attachment.kind !== 'vacant') {
    const epoch: string = attachment.cryptoEpoch;
    // @ts-expect-error Epochs cannot be confused with imprecise JavaScript floats.
    const numericEpoch: number = attachment.cryptoEpoch;
    // @ts-expect-error The native result contains no file key.
    const key = attachment.key;
    // @ts-expect-error A reply is not a mutable source/outbox descriptor.
    attachment.messageId = 'another';
    return epoch;
  }
}
